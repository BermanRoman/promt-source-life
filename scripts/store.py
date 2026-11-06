#!/usr/bin/env python3
"""Private local records and a durable, connector-neutral Drive outbox (stdlib)."""
import argparse
import contextlib
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid


class StoreError(Exception):
    pass


def sha(data):
    return hashlib.sha256(data).hexdigest()


def key_checked(key):
    if key in ('00-справка.md', '00-state.md'):
        return key
    if re.fullmatch(r'undated-[A-Za-z0-9_-]{1,80}\.md', key):
        return key
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}\.md', key):
        try:
            datetime.date.fromisoformat(key[:-3])
            return key
        except ValueError:
            pass
    raise StoreError('invalid record key')


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,200}', value):
        raise StoreError('invalid identifier')
    return value


def digest_checked(value):
    if value is not None and not re.fullmatch('[0-9a-f]{64}', value):
        raise StoreError('expected lowercase SHA-256 or absent')
    return value


def safe_path(path):
    p = Path(os.path.abspath(os.fspath(path)))
    # Reject traversal rather than silently normalizing it.
    if '..' in Path(path).parts:
        raise StoreError('parent traversal is forbidden')
    for part in (p, *p.parents):
        if part.is_symlink():
            raise StoreError('symlinks are forbidden')
    return p


def read_bytes(path):
    p = safe_path(path)
    try:
        fd = os.open(p, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as f:
            return f.read()
    except OSError:
        raise StoreError('cannot read input file') from None


def export_new(path, content):
    p = safe_path(path)
    try:
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(fd, 'wb') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
    except OSError:
        raise StoreError('output must be a new regular file in an existing directory') from None


class Store:
    def __init__(self, root, initialize=False):
        self.root = safe_path(root)
        repository = Path(__file__).absolute().parents[1]
        if self.root == repository or repository in self.root.parents:
            raise StoreError('private runtime data must be outside this repository')
        if initialize:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not self.root.is_dir():
            raise StoreError('initialize the private root first')
        os.chmod(self.root, 0o700)
        db = self.root / 'store.sqlite3'
        for suffix in ('', '-journal', '-wal', '-shm'):
            safe_path(str(db) + suffix)
        if not initialize and not db.is_file():
            raise StoreError('initialize the private root first')
        # Precreate with private permissions; SQLite must never create a public DB.
        fd = os.open(db, os.O_RDWR | (os.O_CREAT if initialize else 0) | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        os.close(fd)
        os.chmod(db, 0o600)
        self.db = sqlite3.connect(db, isolation_level=None, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA secure_delete=ON')
        self.db.execute('PRAGMA temp_store=MEMORY')
        if initialize:
            self.db.executescript('''
                CREATE TABLE IF NOT EXISTS records (
                    key TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                    content BLOB, sha TEXT, remote_id TEXT, base_sha TEXT,
                    deleted INTEGER NOT NULL DEFAULT 0, shared_revision INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, key TEXT NOT NULL, revision INTEGER NOT NULL,
                    kind TEXT NOT NULL, state TEXT NOT NULL, payload BLOB, sha TEXT,
                    base_sha TEXT, remote_id TEXT, operation_id TEXT NOT NULL,
                    target_operation_id TEXT);
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS conflicts (
                    job_id TEXT PRIMARY KEY, remote_content BLOB NOT NULL,
                    remote_sha TEXT NOT NULL, remote_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT OR IGNORE INTO settings VALUES ('sync', 'enabled');
                PRAGMA user_version=1;
            ''')
            self.db.execute("INSERT OR IGNORE INTO settings VALUES ('store_id', ?)", (uuid.uuid4().hex,))
        if self.db.execute('PRAGMA user_version').fetchone()[0] != 1:
            raise StoreError('unsupported store schema')

    def close(self):
        self.db.close()

    def stable_id(self, operation_id):
        namespace = self.db.execute("SELECT value FROM settings WHERE key='store_id'").fetchone()[0]
        return namespace + ':' + operation_id

    def job_stable_id(self, job):
        stable = self.stable_id(job['operation_id'])
        # One forget operation may have several independently confirmed copies.
        return stable + ':' + job['id'] if job['kind'] == 'delete' else stable

    @contextlib.contextmanager
    def tx(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def record(self, key):
        return self.db.execute('SELECT * FROM records WHERE key=?', (key_checked(key),)).fetchone()

    def job(self, job_id):
        row = self.db.execute('SELECT * FROM jobs WHERE id=?', (identifier(job_id),)).fetchone()
        if row is None:
            raise StoreError('unknown job')
        return row

    def replay(self, op, fingerprint):
        identifier(op)
        old = self.db.execute('SELECT * FROM operations WHERE id=?', (op,)).fetchone()
        if old:
            if old['fingerprint'] != fingerprint:
                raise StoreError('operation ID reused with different arguments')
            return json.loads(old['result'])

    def remember(self, op, fingerprint, result):
        self.db.execute('INSERT INTO operations VALUES (?,?,?)', (op, fingerprint, json.dumps(result)))
        return result

    def put(self, key, content, expected_revision, operation_id):
        key_checked(key)
        if type(expected_revision) is not int or expected_revision < 0:
            raise StoreError('revision must be a nonnegative integer')
        try:
            content.decode('utf-8')
        except (UnicodeError, AttributeError):
            raise StoreError('content must be UTF-8 bytes') from None
        fingerprint = sha(json.dumps(['put', key, sha(content), expected_revision]).encode())
        with self.tx():
            replay = self.replay(operation_id, fingerprint)
            if replay is not None:
                return replay
            old = self.record(key)
            if old and old['deleted']:
                raise StoreError('record is forgotten; use a new key')
            if (old['revision'] if old else 0) != expected_revision:
                raise StoreError('revision conflict')
            revision = expected_revision + 1
            digest = sha(content)
            self.db.execute('''INSERT INTO records(key,revision,content,sha) VALUES (?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET revision=excluded.revision,content=excluded.content,sha=excluded.sha''',
                (key, revision, content, digest))
            # Only unsent jobs can be superseded. In-flight jobs remain reconcilable.
            self.db.execute("UPDATE jobs SET state='superseded',payload=NULL WHERE key=? AND state='pending' AND kind='put'", (key,))
            job_id = uuid.uuid4().hex
            self.db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (job_id, key, revision, 'put', 'pending', content, digest,
                 old['base_sha'] if old else None, old['remote_id'] if old else None, operation_id, None))
            return self.remember(operation_id, fingerprint, {'key': key, 'revision': revision, 'job': job_id, 'sha': digest})

    def status(self, row):
        states = {r[0] for r in self.db.execute("SELECT state FROM jobs WHERE key=? AND state IN ('pending','writing','unknown','conflict')", (row['key'],))}
        if 'conflict' in states:
            return 'conflict'
        if states & {'unknown', 'writing'}:
            return 'unknown'
        if row['deleted']:
            return 'deletion-pending' if states else 'deleted'
        return 'both' if not states and row['remote_id'] and row['base_sha'] == row['sha'] else 'local'

    def listing(self):
        return [{'key': r['key'], 'revision': r['revision'], 'sha': r['sha'], 'status': self.status(r)}
                for r in self.db.execute('SELECT * FROM records ORDER BY key')]

    def pending(self):
        rows = [dict(r) for r in self.db.execute("""SELECT id,key,revision,kind,state,sha,base_sha,remote_id,
            operation_id,target_operation_id FROM jobs WHERE state IN ('pending','writing','unknown','conflict') ORDER BY rowid""")]
        for row in rows:
            row['stable_id'] = self.job_stable_id(row)
            row['target_stable_id'] = self.stable_id(row['target_operation_id']) if row['target_operation_id'] else None
        return rows

    def payload(self, job_id):
        job = self.job(job_id)
        if job['kind'] != 'put' or job['payload'] is None:
            raise StoreError('job has no retained payload')
        return job['payload']

    def sync(self, mode):
        if mode not in ('enabled', 'paused', 'revoked'):
            raise StoreError('invalid sync mode')
        with self.tx():
            self.db.execute("UPDATE settings SET value=? WHERE key='sync'", (mode,))
        return {'sync': mode}

    def prepare(self, job_id, observed_sha, remote_id=None):
        digest_checked(observed_sha)
        if remote_id is not None:
            identifier(remote_id)
        with self.tx():
            if self.db.execute("SELECT value FROM settings WHERE key='sync'").fetchone()[0] != 'enabled':
                raise StoreError('sync is paused or revoked')
            job = self.job(job_id)
            if job['state'] != 'pending':
                raise StoreError('job must be pending; reconcile in-flight operations')
            blocked = self.db.execute("SELECT 1 FROM jobs WHERE key=? AND id<>? AND state IN ('writing','unknown','conflict')", (job['key'], job_id)).fetchone()
            if blocked:
                raise StoreError('resolve earlier in-flight job or conflict first')
            if job['remote_id'] and remote_id != job['remote_id']:
                raise StoreError('remote ID mismatch')
            if job['kind'] == 'put':
                if job['remote_id'] is None and (remote_id is not None or observed_sha is not None):
                    raise StoreError('create requires confirmed absence by stable operation ID')
                if observed_sha != job['base_sha']:
                    raise StoreError('remote changed; save a conflict with readback before writing')
            else:
                # Unknown create must be located by its stable operation ID before deletion.
                if not remote_id:
                    raise StoreError('locate remote ID before deleting; or confirm absence with ack-delete')
                if job['remote_id'] is None:
                    self.db.execute('UPDATE jobs SET remote_id=? WHERE id=?', (remote_id, job_id))
            self.db.execute("UPDATE jobs SET state='writing' WHERE id=?", (job_id,))
            return {'job': job_id, 'action': job['kind'], 'operation_id': job['operation_id'],
                    'stable_id': self.job_stable_id(job),
                    'remote_id': remote_id, 'conditional_sha': observed_sha}

    def unknown(self, job_id):
        with self.tx():
            job = self.job(job_id)
            if job['state'] not in ('writing', 'unknown'):
                raise StoreError('only an in-flight job can become unknown')
            self.db.execute("UPDATE jobs SET state='unknown' WHERE id=?", (job_id,))
        return {'job': job_id, 'state': 'unknown'}

    def ack(self, job_id, remote_id, readback):
        identifier(remote_id)
        with self.tx():
            job = self.job(job_id)
            if job['kind'] != 'put' or job['state'] not in ('writing', 'unknown', 'done'):
                raise StoreError('ack requires a prepared write or unknown outcome')
            if job['remote_id'] and job['remote_id'] != remote_id:
                raise StoreError('remote ID mismatch')
            if (job['state'] != 'done' and job['payload'] != readback) or sha(readback) != job['sha']:
                raise StoreError('readback differs from queued bytes')
            record = self.record(job['key'])
            if record['deleted']:
                raise StoreError('forgotten record cannot be acknowledged or resurrected')
            self.db.execute("UPDATE jobs SET state='done',remote_id=?,payload=NULL WHERE id=?", (remote_id, job_id))
            # Replaying an old ack must never regress the shared base after a later ack.
            newer = self.db.execute("SELECT 1 FROM jobs WHERE key=? AND kind='put' AND state='done' AND revision>?", (job['key'], job['revision'])).fetchone()
            if not newer and job['revision'] >= record['shared_revision']:
                self.db.execute('UPDATE records SET remote_id=?,base_sha=?,shared_revision=? WHERE key=?', (remote_id, job['sha'], job['revision'], job['key']))
                self.db.execute("UPDATE jobs SET remote_id=?,base_sha=? WHERE key=? AND state='pending' AND kind='put'", (remote_id, job['sha'], job['key']))
            return {'job': job_id, 'revision': job['revision'], 'status': self.status(self.record(job['key']))}

    def reconcile(self, job_id, remote_id, readback):
        # Reconciliation is a read-only remote lookup + readback, never another create.
        return self.ack(job_id, remote_id, readback)

    def conflict(self, job_id, remote_id, readback):
        identifier(remote_id)
        with self.tx():
            job = self.job(job_id)
            if job['kind'] != 'put' or job['state'] not in ('pending', 'writing', 'unknown', 'conflict'):
                raise StoreError('job cannot enter conflict')
            if job['remote_id'] and job['remote_id'] != remote_id:
                raise StoreError('remote ID mismatch')
            previous = self.db.execute('SELECT * FROM conflicts WHERE job_id=?', (job_id,)).fetchone()
            if previous and (previous['remote_content'] != readback or previous['remote_id'] != remote_id):
                raise StoreError('conflict already retains another remote version; export and resolve it first')
            self.db.execute('INSERT OR IGNORE INTO conflicts VALUES (?,?,?,?)', (job_id, readback, sha(readback), remote_id))
            self.db.execute("UPDATE jobs SET state='conflict' WHERE id=?", (job_id,))
        return {'job': job_id, 'status': 'conflict', 'remote_sha': sha(readback)}

    def resolve(self, job_id, content, expected_revision, operation_id):
        """Explicit reviewed merge: preserve remote base, queue new local revision."""
        with self.tx():
            fingerprint = sha(json.dumps(['resolve', job_id, sha(content), expected_revision]).encode())
            replay = self.replay(operation_id, fingerprint)
            if replay is not None:
                return replay
            job = self.job(job_id)
            conflict = self.db.execute('SELECT * FROM conflicts WHERE job_id=?', (job_id,)).fetchone()
            if job['state'] != 'conflict' or conflict is None:
                raise StoreError('no conflict to resolve')
            record = self.record(job['key'])
            if record['deleted'] or record['revision'] != expected_revision:
                raise StoreError('revision conflict')
            # put is its own transaction; all of the merge must share this one.
            key = job['key']
            try:
                content.decode('utf-8')
            except UnicodeError:
                raise StoreError('content must be UTF-8 bytes') from None
            revision = expected_revision + 1
            self.db.execute('UPDATE records SET revision=?,content=?,sha=?,remote_id=?,base_sha=?,shared_revision=? WHERE key=?',
                (revision, content, sha(content), conflict['remote_id'], conflict['remote_sha'], expected_revision, key))
            self.db.execute("UPDATE jobs SET state='superseded',payload=NULL WHERE key=? AND state IN ('pending','conflict')", (key,))
            new_job = uuid.uuid4().hex
            self.db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)', (new_job, key, revision, 'put', 'pending', content, sha(content), conflict['remote_sha'], conflict['remote_id'], operation_id, None))
            return self.remember(operation_id, fingerprint, {'key': key, 'revision': revision, 'job': new_job, 'sha': sha(content)})

    def pull(self, key, content, expected_revision, remote_id, operation_id):
        key_checked(key)
        identifier(remote_id)
        content.decode('utf-8')
        fingerprint = sha(json.dumps(['pull', key, sha(content), expected_revision, remote_id]).encode())
        with self.tx():
            replay = self.replay(operation_id, fingerprint)
            if replay is not None:
                return replay
            record = self.record(key)
            if record and record['deleted']:
                raise StoreError('tombstone prevents resurrection')
            if (record['revision'] if record else 0) != expected_revision:
                raise StoreError('revision conflict')
            if record and record['remote_id'] != remote_id:
                raise StoreError('remote ID mismatch')
            if self.db.execute("SELECT 1 FROM jobs WHERE key=? AND state IN ('pending','writing','unknown','conflict')", (key,)).fetchone():
                raise StoreError('local pending changes; preserve a conflict instead')
            revision = expected_revision + 1
            self.db.execute('''INSERT INTO records(key,revision,content,sha,remote_id,base_sha,shared_revision) VALUES (?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET
                revision=excluded.revision,content=excluded.content,sha=excluded.sha,remote_id=excluded.remote_id,base_sha=excluded.base_sha,shared_revision=excluded.shared_revision''',
                (key, revision, content, sha(content), remote_id, sha(content), revision))
            return self.remember(operation_id, fingerprint, {'key': key, 'revision': revision, 'status': 'both'})

    def forget(self, key, expected_revision, operation_id):
        key_checked(key)
        fingerprint = sha(json.dumps(['forget', key, expected_revision]).encode())
        with self.tx():
            replay = self.replay(operation_id, fingerprint)
            if replay is not None:
                return replay
            record = self.record(key)
            if record is None or record['revision'] != expected_revision:
                raise StoreError('revision conflict')
            if record['deleted']:
                raise StoreError('record already forgotten')
            # Gather every known copy before erasing conflict bytes. A create can
            # have moved from unknown to conflict without ever acquiring a record ID.
            sources = self.db.execute('''SELECT j.*, c.remote_id AS conflict_remote_id
                FROM jobs j LEFT JOIN conflicts c ON c.job_id=j.id WHERE j.key=?''', (key,)).fetchall()
            active = {'pending', 'writing', 'unknown', 'conflict'}
            remote_ids = {record['remote_id']} if record['remote_id'] else set()
            targets = set()
            for source in sources:
                if source['conflict_remote_id']:
                    remote_ids.add(source['conflict_remote_id'])
                if source['state'] in active and source['remote_id']:
                    remote_ids.add(source['remote_id'])
                if (source['kind'] == 'put' and source['remote_id'] is None
                        and source['state'] in {'writing', 'unknown', 'conflict'}):
                    targets.add((source['conflict_remote_id'], source['operation_id']))
            covered_ids = {remote for remote, _ in targets if remote}
            targets.update((remote, None) for remote in remote_ids - covered_ids)
            if not targets:
                targets.add((None, None))
            self.db.execute('DELETE FROM conflicts WHERE job_id IN (SELECT id FROM jobs WHERE key=?)', (key,))
            self.db.execute("UPDATE jobs SET state='cancelled',payload=NULL,sha=NULL,base_sha=NULL WHERE key=?", (key,))
            revision = expected_revision + 1
            self.db.execute('UPDATE records SET content=NULL,sha=NULL,base_sha=NULL,deleted=1,revision=? WHERE key=?', (revision, key))
            deletion_jobs = []
            for remote_id, target_operation in sorted(targets, key=lambda target: (target[0] or '', target[1] or '')):
                job_id = uuid.uuid4().hex
                self.db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (job_id, key, revision, 'delete', 'pending', None, None, None, remote_id,
                     operation_id, target_operation))
                deletion_jobs.append(job_id)
            result = self.remember(operation_id, fingerprint, {'key': key, 'revision': revision,
                'job': deletion_jobs[0], 'jobs': deletion_jobs, 'status': 'deletion-pending'})
        # secure_delete erases freed cells; VACUUM also removes free pages in the canonical DB.
        self.db.execute('VACUUM')
        return result

    def ack_delete(self, job_id, remote_id, confirmed_absent=False):
        if not confirmed_absent:
            raise StoreError('connector must confirm remote absence')
        if remote_id:
            identifier(remote_id)
        with self.tx():
            job = self.job(job_id)
            if job['kind'] != 'delete' or job['state'] not in ('pending', 'writing', 'unknown', 'done'):
                raise StoreError('not an active deletion')
            if job['remote_id'] and remote_id != job['remote_id']:
                raise StoreError('remote ID mismatch')
            if job['target_operation_id'] and not remote_id:
                raise StoreError('reconcile uncertain create by stable operation ID before confirming absence')
            self.db.execute("UPDATE jobs SET state='done',remote_id=? WHERE id=?", (remote_id, job_id))
            return {'job': job_id, 'status': self.status(self.record(job['key']))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, help='private runtime directory, outside the repository')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('init', 'list', 'pending'):
        sub.add_parser(name)
    for name in ('put', 'pull', 'forget'):
        p = sub.add_parser(name)
        p.add_argument('--key', required=True)
        p.add_argument('--expected-revision', required=True, type=int)
        p.add_argument('--operation-id', required=True)
        if name != 'forget':
            p.add_argument('--input', required=True)
        if name == 'pull':
            p.add_argument('--remote-id', required=True)
    p = sub.add_parser('read')
    p.add_argument('--key', required=True)
    p.add_argument('--output', required=True)
    p = sub.add_parser('payload')
    p.add_argument('--job', required=True)
    p.add_argument('--output', required=True)
    p = sub.add_parser('conflict-read')
    p.add_argument('--job', required=True)
    p.add_argument('--output', required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--job', required=True)
    p.add_argument('--observed-sha', required=True, help='SHA-256 of fresh remote readback, or absent')
    p.add_argument('--remote-id')
    p = sub.add_parser('unknown')
    p.add_argument('--job', required=True)
    for name in ('ack', 'reconcile', 'conflict'):
        p = sub.add_parser(name)
        p.add_argument('--job', required=True)
        p.add_argument('--remote-id', required=True)
        p.add_argument('--readback', required=True)
    p = sub.add_parser('resolve')
    p.add_argument('--job', required=True)
    p.add_argument('--input', required=True)
    p.add_argument('--expected-revision', required=True, type=int)
    p.add_argument('--operation-id', required=True)
    p = sub.add_parser('ack-delete')
    p.add_argument('--job', required=True)
    p.add_argument('--remote-id')
    p.add_argument('--confirmed-absent', action='store_true')
    p = sub.add_parser('sync')
    p.add_argument('mode', choices=('enabled', 'paused', 'revoked'))
    args = parser.parse_args(argv)
    store = None
    try:
        store = Store(args.root, initialize=args.command == 'init')
        command = args.command
        if command == 'init':
            result = {'schema': 1, 'store_id': store.db.execute("SELECT value FROM settings WHERE key='store_id'").fetchone()[0],
                      'sync': store.db.execute("SELECT value FROM settings WHERE key='sync'").fetchone()[0]}
        elif command in ('list', 'pending'):
            result = store.listing() if command == 'list' else store.pending()
        elif command in ('put', 'pull'):
            kwargs = dict(key=args.key, content=read_bytes(args.input), expected_revision=args.expected_revision, operation_id=args.operation_id)
            if command == 'pull':
                kwargs['remote_id'] = args.remote_id
            result = getattr(store, command)(**kwargs)
        elif command == 'forget':
            result = store.forget(args.key, args.expected_revision, args.operation_id)
        elif command == 'read':
            record = store.record(args.key)
            if record is None or record['deleted']:
                raise StoreError('record is absent or forgotten')
            export_new(args.output, record['content'])
            result = {'key': args.key, 'revision': record['revision'], 'status': store.status(record)}
        elif command == 'payload':
            export_new(args.output, store.payload(args.job))
            result = {'job': args.job, 'exported': True}
        elif command == 'conflict-read':
            row = store.db.execute('SELECT remote_content FROM conflicts WHERE job_id=?', (identifier(args.job),)).fetchone()
            if row is None:
                raise StoreError('no retained remote conflict')
            export_new(args.output, row['remote_content'])
            result = {'job': args.job, 'exported': True}
        elif command == 'prepare':
            result = store.prepare(args.job, None if args.observed_sha == 'absent' else args.observed_sha, args.remote_id)
        elif command == 'unknown':
            result = store.unknown(args.job)
        elif command in ('ack', 'reconcile', 'conflict'):
            result = getattr(store, command)(args.job, args.remote_id, read_bytes(args.readback))
        elif command == 'resolve':
            result = store.resolve(args.job, read_bytes(args.input), args.expected_revision, args.operation_id)
        elif command == 'ack-delete':
            result = store.ack_delete(args.job, args.remote_id, args.confirmed_absent)
        elif command == 'sync':
            result = store.sync(args.mode)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (StoreError, OSError, sqlite3.Error, UnicodeError, ValueError):
        # No SQL, payload, filesystem exception, or personal text in logs.
        print('store: operation rejected; check arguments, revision, sync state and connector evidence', file=sys.stderr)
        return 2
    finally:
        if store is not None:
            store.close()


if __name__ == '__main__':
    sys.exit(main())
