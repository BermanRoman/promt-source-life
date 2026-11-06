#!/usr/bin/env python3
"""Validate and read declared local components; stdlib, no network or checkout.

Byte validation and exact ATX selection adapted from prompt-management's
scripts/copywriting.py (itself based on promt-gkkd context helpers).
"""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import unicodedata

REPOSITORY = Path(__file__).resolve().parents[1]
MANIFEST = REPOSITORY / 'dependencies.json'
LOCAL_CANONS = frozenset(('master-prompt.md', 'behavior-methodology.md', 'style-core.md',
                           'spin-core.md', 'addiction-questions.md', 'diary.md'))
ROUTES = {
    'core': 'prompt-management', 'relationships': 'prompt-relationships',
    'job-search': 'prompt-job-search', 'hiring': 'promt-hiring-staff',
    'audience': 'prompt-audience-research', 'sales-training': 'prompt-spin-training',
}
REQUIRED_FILES = {'core': ['management-methodology.md'],
 'relationships': ['relationships.md',
                   'relationships-methodology.md',
                   'workflow.md',
                   'references/boundaries.md',
                   'roles/reader.md',
                   'roles/reviewer.md',
                   'templates/state.md'],
 'job-search': ['job-search.md',
                'job-search-methodology.md',
                'workflow.md',
                'campaign.md',
                'resume.md',
                'contracts/work-evidence-v1.md',
                'templates/task.md',
                'templates/campaign.md',
                'templates/resume-facts.md',
                'roles/reviewer.md',
                'roles/target-reader.md',
                'scripts/job_ledger.py',
                'scripts/README.md',
                'docs/ats-research.md',
                'docs/channels-research.md'],
 'hiring': ['prompt-hiring.md',
            'workflow.md',
            'references/methodology.md',
            'references/hh.md',
            'contracts/work-evidence-v1.md',
            'templates/task.md',
            'roles/writer.md',
            'roles/reviewer.md',
            'roles/target-reader.md',
            'scripts/workload.py',
            'scripts/check_pair.py',
            'docs/tools.md',
            'docs/copywriting.md'],
 'audience': ['audience-research.md',
              'workflow.md',
              'evidence-contract.md',
              'templates/state.md',
              'roles/research-reviewer.md',
              'references/00-context.md',
              'references/01-segment.md',
              'references/02-portrait.md',
              'references/03-custdev.md',
              'references/04-meanings.md',
              'references/05-offer.md',
              'references/06-landing.md'],
 'sales-training': ['spin-training.md', 'spin-methodology.md']}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def valid_path(value):
    if (not isinstance(value, str) or not value or '\\' in value or
            any(ord(c) < 32 or unicodedata.category(c) == 'Cf' for c in value) or
            PurePosixPath(value).is_absolute() or
            any(p in ('', '.', '..') for p in value.split('/'))):
        raise ValueError('invalid relative file path')
    return value


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate manifest key')
        result[key] = value
    return result


def read_manifest(path):
    data = json.loads(path.read_text(encoding='utf-8'), object_pairs_hook=unique_object)
    if (not isinstance(data, dict) or set(data) != {'version', 'routes'} or
            type(data['version']) is not int or data['version'] != 1 or
            not isinstance(data['routes'], dict) or set(data['routes']) != set(ROUTES)):
        raise ValueError('invalid manifest routes/version')
    for route, item in data['routes'].items():
        if (not isinstance(item, dict) or set(item) != {'repository', 'revision', 'files'} or
                item['repository'] != ROUTES[route] or
                not isinstance(item['revision'], str) or
                not re.fullmatch('[0-9a-f]{40}', item['revision']) or
                not isinstance(item['files'], dict) or
                set(item['files']) != set(REQUIRED_FILES[route])):
            raise ValueError('invalid route pin: ' + route)
        for name, digest in item['files'].items():
            valid_path(name)
            if not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest):
                raise ValueError('invalid file SHA-256')
    pair = [data['routes'][r]['files'].get('contracts/work-evidence-v1.md')
            for r in ('job-search', 'hiring')]
    if not pair[0] or pair[0] != pair[1]:
        raise ValueError('work-evidence contract pins must be byte-identical')
    return data['routes']


def git(root, *args):
    return subprocess.run(
        ['git', '--no-replace-objects', '--no-optional-locks', '-C', str(root), *args],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        check=True, timeout=10,
    ).stdout


def check_dependency(root, item):
    root = Path(root).absolute()
    if root != root.resolve() or not root.is_dir():
        raise ValueError('root must be a real checkout directory without symlinks')
    top = Path(git(root, 'rev-parse', '--show-toplevel').decode().strip()).resolve()
    if top != root:
        raise ValueError('root must equal the Git top level')
    if git(root, 'rev-parse', 'HEAD').decode().strip() != item['revision']:
        raise ValueError('checkout HEAD differs from pinned revision')
    sources = {}
    for name, digest in item['files'].items():
        valid_path(name)
        parts = PurePosixPath(name).parts
        target = root / name
        if (any(root.joinpath(*parts[:i]).is_symlink() for i in range(1, len(parts) + 1)) or
                not target.is_file() or root not in target.resolve().parents):
            raise ValueError(name + ': missing or invalid required file')
        entry = git(root, 'ls-tree', '-z', item['revision'], '--', name)
        if not entry.startswith((b'100644 blob ', b'100755 blob ')):
            raise ValueError(name + ': pinned source is not a regular file')
        actual = target.read_bytes()
        pinned = git(root, 'show', item['revision'] + ':' + name)
        # Read bytes directly: assume-unchanged/skip-worktree can hide edits.
        if actual != pinned or sha(actual) != digest:
            raise ValueError(name + ': bytes differ from pinned source/SHA-256')
        sources[name] = actual
    return sources


def headings(text):
    """Return (exact label, depth, character offset), ignoring fenced blocks."""
    if any(unicodedata.category(c) == 'Cf' for c in text):
        raise ValueError('source contains hidden format characters')
    result, fence, offset = [], None, 0
    for line in text.splitlines(keepends=True):
        raw = line.rstrip('\r\n')
        marker = re.match(r'^ {0,3}(`{3,}|~{3,})(.*)$', raw)
        if marker:
            run, tail = marker.groups()
            if fence is None:
                # A backtick fence's info string cannot contain backticks.
                if run[0] != '`' or '`' not in tail:
                    fence = (run[0], len(run))
            elif run[0] == fence[0] and len(run) >= fence[1] and not tail.strip():
                fence = None
        elif fence is None:
            match = re.match(r'^ {0,3}(#{1,6})(?:[ \t]+(.*)|[ \t]*)$', raw)
            if match:
                label = re.sub(r'(?:^|[ \t]+)#+[ \t]*$', '', match[2] or '').strip()
                result.append((label, len(match[1]), offset))
        offset += len(line)
    return result


def sections(text, selectors):
    if len(set(selectors)) != len(selectors):
        raise ValueError('duplicate heading selector')
    table = headings(text)
    selected = []
    for selector in selectors:
        matches = [i for i, row in enumerate(table) if row[0] == selector]
        if len(matches) != 1:
            raise ValueError('heading must occur exactly once: ' + selector)
        index = matches[0]
        _, level, start = table[index]
        end = next((pos for _, depth, pos in table[index + 1:] if depth <= level), len(text))
        selected.append((start, end))
    merged = []
    for start, end in sorted(selected):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return '\n'.join(text[start:end] for start, end in merged)


def local_source(repository, name):
    valid_path(name)
    parts = PurePosixPath(name).parts
    if not (name in LOCAL_CANONS or
            (parts[0] in ('core', 'methods') and name.endswith('.md'))):
        raise ValueError('local reading permits only declared canons and core/methods Markdown')
    if any(p.startswith('.') or p.lower() in ('private', 'runs', 'archives', 'archive', 'secrets')
           for p in parts):
        raise ValueError('private/local working paths are excluded')
    root = Path(repository).absolute()
    target = root / name
    if (root != root.resolve() or
            any(root.joinpath(*parts[:i]).is_symlink() for i in range(1, len(parts) + 1)) or
            not target.is_file() or root not in target.resolve().parents):
        raise ValueError('local source must stay within the real repository')
    return target.read_bytes()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for command in ('check', 'index', 'extract'):
        sub = commands.add_parser(command)
        sub.add_argument('--repos-root', type=Path, default=REPOSITORY.parent)
        sub.add_argument('--route', action='append', choices=ROUTES, default=[])
        if command != 'check':
            sub.add_argument('file', metavar='FILE')
            sub.add_argument('--local', action='store_true', help='read allowed source-life Markdown')
        if command == 'extract':
            sub.add_argument('--heading', action='append', default=[], help='exact ATX label; repeatable')
    args = parser.parse_args(argv)
    try:
        manifest = read_manifest(MANIFEST)
        if len(set(args.route)) != len(args.route):
            raise ValueError('duplicate route')
        if args.command == 'check':
            if not args.route:
                raise ValueError('check requires at least one --route')
            for route in args.route:
                item = manifest[route]
                check_dependency(args.repos_root / item['repository'], item)
            result = ''.join(route + ': ok\n' for route in args.route)
        else:
            if args.local:
                if args.route:
                    raise ValueError('--local cannot be combined with --route')
                data = local_source(REPOSITORY, args.file)
            else:
                if len(args.route) != 1:
                    raise ValueError('external reading requires exactly one --route')
                item = manifest[args.route[0]]
                if valid_path(args.file) not in item['files']:
                    raise ValueError('file is not declared for selected route')
                data = check_dependency(args.repos_root / item['repository'], item)[args.file]
            source = data.decode('utf-8')
            if args.command == 'index':
                result = ''.join('#' * level + ' ' + label + '\n'
                                 for label, level, _ in headings(source))
            else:
                headings(source)  # validate even when returning the whole file
                result = sections(source, args.heading) if args.heading else source
            print('read SHA-256: ' + sha(data) + '; selected SHA-256: ' +
                  sha(result.encode('utf-8')) + '; characters: ' + str(len(result)), file=sys.stderr)
    except (OSError, UnicodeError, ValueError, TypeError, subprocess.SubprocessError) as error:
        detail = str(error) if isinstance(error, ValueError) else type(error).__name__
        print('components: ' + detail, file=sys.stderr)
        return 2
    sys.stdout.write(result)
    return 0


if __name__ == '__main__':
    sys.exit(main())
