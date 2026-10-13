from datetime import datetime, timezone
import pytest
from ecommerce.events import EventGenerator, validate, timestamp
from ecommerce.managed_producer import scenario_events
from ecommerce.control import enqueue


def test_duplicate_pairs_have_identical_payloads():
    records = scenario_events(EventGenerator(), 'duplicates')
    assert len({r['event_id'] for r in records}) == 50
    assert all(records[i] == records[i+1] for i in range(0, 100, 2))


def test_flash_sale_and_late_records():
    records = scenario_events(EventGenerator(), 'burst', 1000)
    assert all(validate(r)['event_type'] == 'purchase' for r in records)
    late = scenario_events(EventGenerator(), 'late')
    assert all(179 < (datetime.now(timezone.utc)-timestamp(r['event_time'])).total_seconds() < 190 for r in late)


def test_invalid_scenario_and_unknown_commands():
    for record in scenario_events(EventGenerator(), 'invalid'):
        with pytest.raises(ValueError):
            validate(record)
    with pytest.raises(ValueError):
        enqueue('arbitrary-command')
