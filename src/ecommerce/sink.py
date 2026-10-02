"""Atomic microbatch publication with a ledger and absolute-value upserts."""

import os
import json

UPSERT = """
INSERT INTO window_metrics
(query_name,window_start,window_end,category,product_id,events,purchases,revenue_paise,
 views,carts,active_users_approx,epoch_id)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (query_name,window_start,window_end,category,product_id) DO UPDATE SET
events=EXCLUDED.events,purchases=EXCLUDED.purchases,revenue_paise=EXCLUDED.revenue_paise,
views=EXCLUDED.views,carts=EXCLUDED.carts,active_users_approx=EXCLUDED.active_users_approx,
epoch_id=EXCLUDED.epoch_id,updated_at=now()
WHERE window_metrics.epoch_id <= EXCLUDED.epoch_id
"""


def connection():
    import psycopg

    return psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=10)


def publish(frame, epoch, query_name):
    # Lock prevents overlapping writers using the same query/checkpoint identity.
    # All rows and the ledger commit together. Retrying an epoch becomes a no-op.
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (query_name,))
            cur.execute(
                "SELECT 1 FROM batch_commits WHERE query_name=%s AND epoch_id=%s", (query_name, epoch)
            )
            if cur.fetchone():
                # Drain iterator to allow Spark's stateful batch to complete.
                for _ in frame.toLocalIterator():
                    pass
                return
            for row in frame.toLocalIterator():
                r = row.asDict(recursive=True)
                cur.execute(
                    UPSERT,
                    (
                        query_name,
                        r["window"]["start"],
                        r["window"]["end"],
                        r["category"],
                        r["product_id"],
                        r["events"],
                        r["purchases"],
                        r["revenue_paise"],
                        r["views"],
                        r["carts"],
                        r["active_users_approx"],
                        epoch,
                    ),
                )
            cur.execute("INSERT INTO batch_commits(query_name,epoch_id) VALUES (%s,%s)", (query_name, epoch))


def record_progress(name, progress):
    with connection() as conn:
        conn.execute(
            """INSERT INTO query_progress(query_name,progress,updated_at) VALUES(%s,%s::jsonb,now())
                      ON CONFLICT(query_name) DO UPDATE SET progress=EXCLUDED.progress,updated_at=now()""",
            (name, json.dumps(progress)),
        )
