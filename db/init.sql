CREATE TABLE IF NOT EXISTS window_metrics (
 query_name text NOT NULL, window_start timestamptz NOT NULL, window_end timestamptz NOT NULL,
 category text NOT NULL, product_id text NOT NULL,
 events bigint NOT NULL CHECK(events >= 0), purchases bigint NOT NULL CHECK(purchases >= 0),
 revenue_paise bigint NOT NULL CHECK(revenue_paise >= 0), views bigint NOT NULL CHECK(views >= 0),
 carts bigint NOT NULL CHECK(carts >= 0), active_users_approx bigint NOT NULL CHECK(active_users_approx >= 0),
 epoch_id bigint NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(query_name,window_start,window_end,category,product_id)
);
CREATE INDEX IF NOT EXISTS metrics_recent ON window_metrics(query_name,window_start DESC);
CREATE TABLE IF NOT EXISTS batch_commits (
 query_name text NOT NULL, epoch_id bigint NOT NULL, committed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(query_name,epoch_id)
);
CREATE TABLE IF NOT EXISTS query_progress (
 query_name text PRIMARY KEY, progress jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS batch_reports (
 run_id text PRIMARY KEY, summary jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
