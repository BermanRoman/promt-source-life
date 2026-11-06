"""Offline storage tests: no account, network, connector, or personal fixture."""
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'store.py'
SPEC = importlib.util.spec_from_file_location('life_store', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
Store, StoreError, sha = MODULE.Store, MODULE.StoreError, MODULE.sha


class FakeDrive:
    """Minimal conditional writer with stable operation metadata and readback."""
    def __init__(self):
        self.files = {}
        self.operations = {}
        self.creates = 0

    def write(self, store, job_id):
        job = store.job(job_id)
        remote_id = job['remote_id']
        actual = sha(self.files[remote_id]) if remote_id in self.files else None
        store.prepare(job_id, actual, remote_id)
        if remote_id is None:
            if job['operation_id'] in self.operations:
                raise AssertionError('duplicate create')
            self.creates += 1
            remote_id = 'remote-' + str(self.creates)
            self.operations[job['operation_id']] = remote_id
        self.files[remote_id] = store.payload(job_id)
        return remote_id


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve() / 'private'
        self.store = Store(self.root, initialize=True)
        self.drive = FakeDrive()

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def put(self, text=b'first\n', rev=0, op='put-1', key='2026-11-06.md'):
        return self.store.put(key, text, rev, op)

    def shared(self):
        first = self.put()
        remote = self.drive.write(self.store, first['job'])
        self.store.ack(first['job'], remote, self.drive.files[remote])
        return first, remote

    def test_idempotency_changed_operation_and_cas(self):
        first = self.put()
        self.assertEqual(first, self.put())
        with self.assertRaises(StoreError):
            self.put(b'changed', op='put-1')
        with self.assertRaises(StoreError):
            self.put(b'changed', op='put-2')
        self.assertEqual(len(self.store.pending()), 1)

    def test_cas_concurrency_two_connections(self):
        self.put()
        def writer(number):
            store = Store(self.root)
            try:
                store.put('2026-11-06.md', str(number).encode(), 1, 'concurrent-' + str(number))
                return True
            except StoreError:
                return False
            finally:
                store.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(writer, (1, 2))), [False, True])
        self.assertEqual(self.store.record('2026-11-06.md')['revision'], 2)

    def test_offline_queue_survives_restart_and_no_personal_pending(self):
        self.put(b'unique-private-content')
        self.store.close()
        self.store = Store(self.root)
        self.assertEqual(self.store.listing()[0]['status'], 'local')
        self.assertNotIn('unique-private-content', json.dumps(self.store.pending()))
        self.assertEqual(len(self.store.pending()), 1)

    def test_ack_requires_prepare_and_exact_readback(self):
        first = self.put()
        with self.assertRaises(StoreError):
            self.store.ack(first['job'], 'r1', b'first\n')
        remote = self.drive.write(self.store, first['job'])
        with self.assertRaises(StoreError):
            self.store.ack(first['job'], remote, b'wrong')
        self.assertEqual(self.store.listing()[0]['status'], 'unknown')
        self.store.ack(first['job'], remote, b'first\n')
        self.assertEqual(self.store.listing()[0]['status'], 'both')

    def test_stale_ack_does_not_confirm_new_revision_or_regress_base(self):
        old = self.put()
        remote = self.drive.write(self.store, old['job'])
        newer = self.put(b'new\n', 1, 'put-2')
        self.assertEqual(self.store.ack(old['job'], remote, b'first\n')['status'], 'local')
        self.assertEqual(self.store.job(newer['job'])['base_sha'], sha(b'first\n'))
        self.drive.write(self.store, newer['job'])
        self.store.ack(newer['job'], remote, b'new\n')
        self.store.ack(old['job'], remote, b'first\n')
        self.assertEqual(self.store.record(old['key'])['base_sha'], sha(b'new\n'))
        self.assertEqual(self.store.listing()[0]['status'], 'both')

    def test_unknown_create_is_reconciled_without_duplicate(self):
        first = self.put()
        remote = self.drive.write(self.store, first['job'])
        self.store.unknown(first['job'])
        with self.assertRaises(StoreError):
            self.drive.write(self.store, first['job'])
        found = self.drive.operations['put-1']
        self.store.reconcile(first['job'], found, self.drive.files[found])
        self.assertEqual(self.drive.creates, 1)
        self.assertEqual(found, remote)
        self.assertEqual(self.store.listing()[0]['status'], 'both')

    def test_local_remote_conflict_preserves_both_and_requires_merge(self):
        first, remote = self.shared()
        local = self.put(b'local\n', 1, 'local-edit')
        self.drive.files[remote] = b'remote\n'
        with self.assertRaises(StoreError):
            self.drive.write(self.store, local['job'])
        self.store.conflict(local['job'], remote, self.drive.files[remote])
        self.assertEqual(self.store.listing()[0]['status'], 'conflict')
        self.assertEqual(self.store.payload(local['job']), b'local\n')
        saved = self.store.db.execute('SELECT remote_content FROM conflicts').fetchone()[0]
        self.assertEqual(saved, b'remote\n')
        with self.assertRaises(StoreError):
            self.store.conflict(local['job'], remote, b'newer remote')
        merged = self.store.resolve(local['job'], b'merged\n', 2, 'merge')
        self.assertEqual(merged, self.store.resolve(local['job'], b'merged\n', 2, 'merge'))
        self.store.ack(first['job'], remote, b'first\n')
        self.assertEqual(self.store.job(merged['job'])['base_sha'], sha(b'remote\n'))
        self.drive.write(self.store, merged['job'])
        self.store.ack(merged['job'], remote, b'merged\n')
        self.assertEqual(self.store.listing()[0]['status'], 'both')

    def test_remote_only_pull_cas_and_pending_protection(self):
        first, remote = self.shared()
        result = self.store.pull('2026-11-06.md', b'remote only', 1, remote, 'pull')
        self.assertEqual(result['status'], 'both')
        self.assertEqual(result, self.store.pull('2026-11-06.md', b'remote only', 1, remote, 'pull'))
        self.store.ack(first['job'], remote, b'first\n')
        self.assertEqual(self.store.record(first['key'])['base_sha'], sha(b'remote only'))
        self.assertEqual(self.store.listing()[0]['status'], 'both')
        self.put(b'pending', 2, 'local')
        with self.assertRaises(StoreError):
            self.store.pull('2026-11-06.md', b'other', 3, remote, 'pull2')

    def test_forget_scrubs_history_outbox_conflict_and_awaits_remote(self):
        _, remote = self.shared()
        local = self.put(b'private-marker-delete-me', 1, 'private-edit')
        self.store.conflict(local['job'], remote, b'private-marker-delete-me-remote')
        result = self.store.forget(local['key'], 2, 'forget')
        self.assertEqual(self.store.listing()[0]['status'], 'deletion-pending')
        self.assertIsNone(self.store.record(local['key'])['content'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM conflicts').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM jobs WHERE payload IS NOT NULL').fetchone()[0], 0)
        self.assertNotIn(b'private-marker-delete-me', (self.root / 'store.sqlite3').read_bytes())
        with self.assertRaises(StoreError):
            self.store.ack_delete(result['job'], remote)
        self.store.prepare(result['job'], sha(b'private-marker-delete-me-remote'), remote)
        del self.drive.files[remote]
        self.store.ack_delete(result['job'], remote, True)
        self.assertEqual(self.store.listing()[0]['status'], 'deleted')
        with self.assertRaises(StoreError):
            self.store.pull(local['key'], b'resurrect', 3, remote, 'resurrect')
        with self.assertRaises(StoreError):
            self.put(b'resurrect', 3, 'recreate')

    def test_forget_unknown_create_retains_lookup_identity(self):
        first = self.put()
        remote = self.drive.write(self.store, first['job'])
        self.store.unknown(first['job'])
        deletion = self.store.forget(first['key'], 1, 'forget')
        self.assertEqual(self.store.job(deletion['job'])['target_operation_id'], 'put-1')
        with self.assertRaises(StoreError):
            self.store.ack_delete(deletion['job'], None, True)
        with self.assertRaises(StoreError):
            self.store.reconcile(first['job'], remote, b'first\n')
        self.store.prepare(deletion['job'], sha(self.drive.files[remote]), remote)
        del self.drive.files[remote]
        self.store.ack_delete(deletion['job'], remote, True)

    def test_forget_unknown_create_conflict_retains_remote_id_and_origin(self):
        first = self.put(b'local', op='create')
        self.store.prepare(first['job'], None)
        self.store.unknown(first['job'])
        self.store.conflict(first['job'], 'drive-file', b'external edit')
        deletion = self.store.forget(first['key'], 1, 'forget')
        job = self.store.job(deletion['job'])
        self.assertEqual(job['remote_id'], 'drive-file')
        self.assertEqual(job['target_operation_id'], 'create')
        self.assertEqual(self.store.pending()[0]['target_stable_id'], self.store.stable_id('create'))
        self.assertIsNone(self.store.record(first['key'])['content'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM conflicts').fetchone()[0], 0)
        for remote in (None, 'different-file'):
            with self.assertRaises(StoreError):
                self.store.ack_delete(deletion['job'], remote, True)
            self.assertEqual(self.store.listing()[0]['status'], 'deletion-pending')
        self.assertEqual(self.store.ack_delete(deletion['job'], 'drive-file', True)['status'], 'deleted')

    def test_forget_multiple_conflict_copies_requires_all_remote_confirmations(self):
        first = self.put(b'local-a', op='create-a')
        self.store.prepare(first['job'], None)
        self.store.unknown(first['job'])
        self.store.conflict(first['job'], 'drive-a', b'external-a')
        second = self.put(b'local-b', 1, 'create-b')
        self.store.conflict(second['job'], 'drive-b', b'external-b')
        deletion = self.store.forget(first['key'], 2, 'forget')
        self.assertEqual(len(deletion['jobs']), 2)
        self.assertEqual(deletion, self.store.forget(first['key'], 2, 'forget'))
        jobs = {self.store.job(job)['remote_id']: job for job in deletion['jobs']}
        self.assertEqual(set(jobs), {'drive-a', 'drive-b'})
        self.assertEqual(len({job['stable_id'] for job in self.store.pending()}), 2)
        self.store.close()
        self.store = Store(self.root)
        with self.assertRaises(StoreError):
            self.store.ack_delete(jobs['drive-a'], None, True)
        first_ack = self.store.ack_delete(jobs['drive-a'], 'drive-a', True)
        self.assertEqual(first_ack['status'], 'deletion-pending')
        self.assertEqual(self.store.listing()[0]['status'], 'deletion-pending')
        with self.assertRaises(StoreError):
            self.store.ack_delete(jobs['drive-b'], 'drive-a', True)
        self.assertEqual(self.store.ack_delete(jobs['drive-b'], 'drive-b', True)['status'], 'deleted')

    def test_pause_revoke_and_resume(self):
        first = self.put()
        for state in ('paused', 'revoked'):
            self.store.sync(state)
            with self.assertRaises(StoreError):
                self.drive.write(self.store, first['job'])
        self.store.sync('enabled')
        self.drive.write(self.store, first['job'])

    def test_validation_permissions_and_exclusive_export(self):
        for key in ('../x', '2026-02-30.md', '/tmp/a', 'undated-../x.md'):
            with self.assertRaises(StoreError):
                self.put(key=key)
        with self.assertRaises(StoreError):
            self.put(rev=float('nan'))
        if os.name == 'posix':
            self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
            self.assertEqual((self.root / 'store.sqlite3').stat().st_mode & 0o777, 0o600)
        link = Path(self.tmp.name).resolve() / 'link'
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(StoreError):
            Store(link)
        output = self.root / 'export.md'
        MODULE.export_new(output, b'text')
        with self.assertRaises(StoreError):
            MODULE.export_new(output, b'other')
        self.assertEqual(output.read_bytes(), b'text')

    def test_cli_round_trip_and_errors_do_not_log_content(self):
        source = self.root / 'input.md'
        source.write_bytes(b'private-cli-marker')
        def cli(*args):
            return subprocess.run([sys.executable, str(SCRIPT), '--root', str(self.root), *args], capture_output=True, text=True)
        output = cli('put', '--key', '00-state.md', '--input', str(source), '--expected-revision', '0', '--operation-id', 'cli-put')
        self.assertEqual(output.returncode, 0, output.stderr)
        job = json.loads(output.stdout)['job']
        self.assertNotIn('private-cli-marker', cli('pending').stdout)
        exported = self.root / 'payload.md'
        self.assertEqual(cli('payload', '--job', job, '--output', str(exported)).returncode, 0)
        self.assertEqual(exported.read_bytes(), source.read_bytes())
        failed = cli('ack', '--job', job, '--remote-id', 'remote', '--readback', str(source))
        self.assertEqual(failed.returncode, 2)
        self.assertNotIn('private-cli-marker', failed.stdout + failed.stderr)


if __name__ == '__main__':
    unittest.main()
