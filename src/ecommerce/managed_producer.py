"""Live generator controlled through PostgreSQL; no Docker socket required."""
import json
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
from psycopg.types.json import Jsonb
from . import control
from .events import EventGenerator
from .replay import read_batch


def scenario_events(generator, kind, count=100):
    events = []
    for i in range(count):
        event = generator.next(trending=kind == 'burst')
        if kind == 'burst':
            event['event_type'] = 'purchase'
        elif kind == 'late':
            event['event_time'] = (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()
        elif kind == 'invalid':
            event = {'schema_version': 1, 'event_type': 'purchase'}
        elif kind == 'duplicates' and i % 2:
            event = events[-1].copy()
        events.append(event)
    return events


def main():
    control.initialize()
    control.recover()
    generator = EventGenerator()
    producer = Producer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092'),
                         'enable.idempotence': True, 'acks': 'all', 'delivery.timeout.ms': 15000})
    stopped = False

    def stop(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopped:
        started = time.monotonic()
        action = None
        try:
            with control.connect() as conn:
                state = conn.execute('SELECT * FROM generator_control WHERE id=1 FOR UPDATE').fetchone()
                pending = None
                if state['source'] == 'real' and state['enabled']:
                    pending = state['replay_pending']
                    if not pending:
                        position, events = read_batch(state['replay_position'], state['rate'])
                        if events:
                            pending = {'position': position, 'events': events}
                            conn.execute('UPDATE generator_control SET replay_pending=%s WHERE id=1', (Jsonb(pending),))
                        else:
                            conn.execute('UPDATE generator_control SET enabled=false WHERE id=1')
            if state['source'] == 'real':
                events = pending['events'] if pending else []
            else:
                action = control.claim()
                events = scenario_events(generator, action['kind'], 1000 if action['kind'] == 'burst' else 100) if action else (
                    scenario_events(generator, 'normal', state['rate']) if state['enabled'] else [])
            confirmed = []
            errors = []
            for event in events:
                def delivered(error, message, payload=event):
                    if error:
                        errors.append(str(error))
                    else:
                        confirmed.append(payload)
                producer.produce(os.getenv('KAFKA_TOPIC', 'ecommerce.events.v1'),
                                 key=event.get('user_id', 'invalid'), value=json.dumps(event).encode(),
                                 on_delivery=delivered)
                producer.poll(0)
            remaining = producer.flush(20)
            with control.connect() as conn:
                conn.execute('UPDATE generator_control SET delivered=delivered+%s, heartbeat=now(), error=NULL WHERE id=1',
                             (len(confirmed),))
                for event in confirmed[-20:]:
                    conn.execute('INSERT INTO recent_events(payload) VALUES(%s)', (Jsonb(event),))
                conn.execute('DELETE FROM recent_events WHERE id < (SELECT coalesce(max(id),0)-199 FROM recent_events)')
                if pending and not errors and not remaining:
                    conn.execute('UPDATE generator_control SET replay_position=%s,replay_pending=NULL WHERE id=1',
                                 (pending['position'],))
            if errors or remaining:
                raise RuntimeError('Kafka delivery incomplete; some events may have arrived. Check pipeline before retrying.')
            if action:
                detail = f'{len(confirmed)} Kafka-confirmed records.'
                if action['kind'] == 'duplicates':
                    detail += ' Includes 50 repeated IDs paired with 50 originals.'
                if action['kind'] == 'late':
                    detail += ' Timestamps are 3 minutes old; acceptance depends on the current watermark.'
                if action['kind'] == 'invalid':
                    detail += ' Missing required fields; Spark routes these records to quarantine.'
                control.finish(action['id'], 'completed', detail)
        except Exception as exc:
            if action:
                control.finish(action['id'], 'failed', str(exc)[:400])
            with control.connect() as conn:
                conn.execute('UPDATE generator_control SET enabled=false,error=%s,heartbeat=now() WHERE id=1', (str(exc)[:400],))
            time.sleep(2)
        time.sleep(max(0, 1 - (time.monotonic() - started)))
    producer.flush(20)


if __name__ == '__main__':
    main()
