"""Database-backed controls shared by the dashboard and dedicated workers."""
import os
import psycopg
from psycopg.rows import dict_row

SCHEMA = """
CREATE TABLE IF NOT EXISTS generator_control (
 id integer PRIMARY KEY CHECK(id=1), enabled boolean NOT NULL DEFAULT true,
 rate integer NOT NULL DEFAULT 100 CHECK(rate BETWEEN 1 AND 500),
 delivered bigint NOT NULL DEFAULT 0, heartbeat timestamptz, error text);
ALTER TABLE generator_control ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'synthetic';
ALTER TABLE generator_control ADD COLUMN IF NOT EXISTS replay_position bigint NOT NULL DEFAULT 0;
ALTER TABLE generator_control ADD COLUMN IF NOT EXISTS replay_pending jsonb;
ALTER TABLE generator_control ADD COLUMN IF NOT EXISTS replay_run bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS control_actions (
 id bigserial PRIMARY KEY, kind text NOT NULL, status text NOT NULL DEFAULT 'queued',
 detail text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now());
CREATE UNIQUE INDEX IF NOT EXISTS one_active_action ON control_actions ((kind='batch'))
 WHERE status IN ('queued','running');
CREATE TABLE IF NOT EXISTS recent_events (
 id bigserial PRIMARY KEY, payload jsonb NOT NULL, delivered_at timestamptz NOT NULL DEFAULT now());
"""


def connect():
    return psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=5, row_factory=dict_row)


def initialize():
    with connect() as conn:
        conn.execute('SELECT pg_advisory_xact_lock(4042026)')
        conn.execute(SCHEMA)
        conn.execute('INSERT INTO generator_control(id,source) VALUES(1,%s) ON CONFLICT DO NOTHING',
                     (os.getenv('DEFAULT_SOURCE', 'synthetic'),))


def enqueue(kind):
    if kind not in {'burst', 'duplicates', 'invalid', 'late', 'batch'}:
        raise ValueError('Unknown action')
    with connect() as conn:
        state = conn.execute('SELECT source FROM generator_control WHERE id=1 FOR UPDATE').fetchone()
        if kind != 'batch' and state['source'] != 'synthetic':
            raise ValueError('Switch to synthetic mode for artificial test scenarios.')
        return conn.execute('INSERT INTO control_actions(kind) VALUES(%s) RETURNING id', (kind,)).fetchone()['id']


def set_source(source):
    if source not in {'real', 'synthetic'}:
        raise ValueError('Unknown source')
    with connect() as conn:
        state = conn.execute('SELECT * FROM generator_control WHERE id=1 FOR UPDATE').fetchone()
        if state['enabled'] or state['replay_pending']:
            raise ValueError('Pause and wait for the current delivery to finish before switching source.')
        if conn.execute("SELECT 1 FROM control_actions WHERE status IN ('queued','running')").fetchone():
            raise ValueError('Wait for the active action to finish before switching source.')
        conn.execute('UPDATE generator_control SET source=%s,error=NULL WHERE id=1', (source,))


def claim(batch=False):
    with connect() as conn:
        return conn.execute("""UPDATE control_actions SET status='running',updated_at=now()
          WHERE id=(SELECT id FROM control_actions WHERE status='queued' AND (kind='batch')=%s
          ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *""", (batch,)).fetchone()


def finish(action_id, status, detail):
    with connect() as conn:
        conn.execute('UPDATE control_actions SET status=%s,detail=%s,updated_at=now() WHERE id=%s',
                     (status, detail, action_id))


def recover(batch=False):
    with connect() as conn:
        conn.execute("""UPDATE control_actions SET status='failed', detail='Worker restarted; action interrupted. Review results before retrying.',
          updated_at=now() WHERE status='running' AND (kind='batch')=%s""", (batch,))


def restart_replay(total):
    """Start a new labelled demonstration run without deleting earlier results."""
    if total <= 0:
        raise ValueError('The replay dataset is unavailable.')
    with connect() as conn:
        state = conn.execute('SELECT * FROM generator_control WHERE id=1 FOR UPDATE').fetchone()
        if state['source'] != 'real' or state['enabled'] or state['replay_pending']:
            raise ValueError('Pause and wait for delivery to finish before starting another replay.')
        if state['replay_position'] < total:
            raise ValueError('Resume the current sample with Start events; restart is available after completion.')
        if conn.execute("SELECT 1 FROM control_actions WHERE status IN ('queued','running')").fetchone():
            raise ValueError('Wait for the active action to finish.')
        conn.execute('UPDATE generator_control SET replay_position=0,replay_run=replay_run+1,enabled=true,error=NULL WHERE id=1')
