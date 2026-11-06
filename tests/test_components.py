"""Synthetic repositories only; no network, real user data or external writes."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('components', Path(__file__).resolve().parents[1] / 'scripts/components.py')
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)


class ComponentsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve()
        self.root = self.base / 'synthetic'
        self.root.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Synthetic Test')
        self.git('config', 'user.email', 'test@example.invalid')
        self.file = self.root / 'source.md'
        self.file.write_text('# One\nA\n## Child\nB\n# Two\nC\n')
        self.git('add', '.')
        self.git('commit', '-qm', 'Synthetic fixture')
        self.item = {'revision': self.git('rev-parse', 'HEAD').decode().strip(),
                     'files': {'source.md': c.sha(self.file.read_bytes())}}

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.root), *args], stderr=subprocess.DEVNULL)

    def test_clean_pin(self):
        self.assertEqual(c.check_dependency(self.root, self.item)['source.md'], self.file.read_bytes())

    def test_hidden_worktree_edits(self):
        for flag in ('--assume-unchanged', '--skip-worktree'):
            with self.subTest(flag=flag):
                self.git('update-index', flag, 'source.md')
                self.file.write_text('modified')
                with self.assertRaises(ValueError):
                    c.check_dependency(self.root, self.item)
                self.file.write_text('# One\nA\n## Child\nB\n# Two\nC\n')
                self.git('update-index', '--no-assume-unchanged', '--no-skip-worktree', 'source.md')

    def test_replace_cannot_forge_pinned_bytes(self):
        original_blob = self.git('rev-parse', 'HEAD:source.md').decode().strip()
        self.file.write_text('forged')
        forged_blob = self.git('hash-object', '-w', 'source.md').decode().strip()
        self.git('replace', original_blob, forged_blob)
        self.item['files']['source.md'] = c.sha(b'forged')
        with self.assertRaises(ValueError):
            c.check_dependency(self.root, self.item)

    def test_wrong_head_digest_and_missing(self):
        for change in ('revision', 'digest', 'missing'):
            with self.subTest(change=change):
                item = {'revision': self.item['revision'], 'files': dict(self.item['files'])}
                if change == 'revision':
                    item['revision'] = '0' * 40
                elif change == 'digest':
                    item['files']['source.md'] = '0' * 64
                else:
                    self.file.unlink()
                with self.assertRaises((ValueError, subprocess.SubprocessError)):
                    c.check_dependency(self.root, item)

    def test_symlink_and_wrong_root(self):
        alias = self.base / 'alias'
        alias.symlink_to(self.root, target_is_directory=True)
        nested = self.root / 'nested'
        nested.mkdir()
        for root in (alias, nested):
            with self.assertRaises(ValueError):
                c.check_dependency(root, self.item)
        self.file.unlink()
        self.file.symlink_to(self.base / 'outside.md')
        with self.assertRaises(ValueError):
            c.check_dependency(self.root, self.item)

    def test_manifest_routes_profiles_pair(self):
        original = json.loads(c.MANIFEST.read_text())
        variants = []
        missing_route = json.loads(json.dumps(original))
        del missing_route['routes']['relationships']
        variants.append(missing_route)
        missing_role = json.loads(json.dumps(original))
        del missing_role['routes']['relationships']['files']['roles/reader.md']
        variants.append(missing_role)
        mismatched = json.loads(json.dumps(original))
        mismatched['routes']['hiring']['files']['contracts/work-evidence-v1.md'] = '0' * 64
        variants.append(mismatched)
        for i, value in enumerate(variants):
            path = self.base / str(i)
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                c.read_manifest(path)
        self.assertEqual(set(c.read_manifest(c.MANIFEST)), set(c.ROUTES))

    def test_exact_sections_fences_and_overlap(self):
        text = '# Root\nA\n## Child\nB\n```md\n# Fake\n```\n# End\nC\n'
        self.assertEqual(c.sections(text, ['Root', 'Child']), text[:text.index('# End')])
        self.assertNotIn('Fake', [r[0] for r in c.headings(text)])
        for selectors in (['Missing'], ['Root', 'Root']):
            with self.assertRaises(ValueError):
                c.sections(text, selectors)
        with self.assertRaises(ValueError):
            c.sections('# Same\na\n# Same\nb\n', ['Same'])
        with self.assertRaises(ValueError):
            c.headings('# Bad\u200b\n')

    def test_local_containment_and_private(self):
        (self.root / 'core').mkdir()
        public = self.root / 'core' / 'public.md'
        public.write_text('# Public\n')
        self.assertEqual(c.local_source(self.root, 'core/public.md'), public.read_bytes())
        for name in ('../source.md', '/source.md', '.env', 'runs/a.md', 'core/private/a.md'):
            with self.assertRaises(ValueError):
                c.local_source(self.root, name)
        public.unlink()
        public.symlink_to(self.file)
        with self.assertRaises(ValueError):
            c.local_source(self.root, 'core/public.md')

    def test_local_canon_allowlist(self):
        for name in c.LOCAL_CANONS:
            (self.root / name).write_text('# Canon\nText\n')
            self.assertEqual(c.local_source(self.root, name), b'# Canon\nText\n')
        for name in ('README.md', 'dependencies.json', 'personal-diary.md'):
            with self.assertRaises(ValueError):
                c.local_source(self.root, name)

    def test_cli_only_selected_route_and_atomic_output(self):
        manifest = {'core': dict(self.item, repository='synthetic'),
                    'relationships': dict(self.item, repository='absent')}
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(c, 'read_manifest', return_value=manifest), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(c.main(['check', '--route', 'core', '--repos-root', str(self.base)]), 0)
        self.assertEqual(stdout.getvalue(), 'core: ok\n')
        stdout = io.StringIO()
        with patch.object(c, 'read_manifest', return_value=manifest), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(c.main(['check', '--route', 'core', '--route', 'relationships', '--repos-root', str(self.base)]), 2)
        self.assertEqual(stdout.getvalue(), '')
        stdout = io.StringIO()
        with patch.object(c, 'read_manifest', return_value=manifest), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(c.main(['extract', 'source.md', '--route', 'core', '--repos-root', str(self.base), '--heading', 'missing']), 2)
        self.assertEqual(stdout.getvalue(), '')


if __name__ == '__main__':
    unittest.main()
