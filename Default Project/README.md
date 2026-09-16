# U1T01: Real-Time Analytics with PostgreSQL CDC & ClickHouse

E-commerce OLTP (PostgreSQL) -> CDC pipeline -> OLAP (ClickHouse),
with continuous load generator, benchmark suite, and live dashboard.
Runs with a **single command**.

## Quickstart

```bash
docker compose up --build
```

That's it. What happens automatically:

1. `postgres` starts with `wal_level=logical`, init schema
   (`init-db/01-schema.sql`): 4 tables (3NF) + `cdc_outbox` +
   triggers + `PUBLICATION cdc_pub`.
2. `clickhouse` starts, creates `shop.*_ch` tables
   (`ReplacingMergeTree(_version)`, `is_deleted` flag).
3. `cdc-worker` snapshots existing rows, then streams `cdc_outbox`
   into ClickHouse in batches (tombstones for DELETEs).
4. `generator` seeds ~5k customers / 500 products / 30k orders
   (~200k total row ops) then pumps ~120 INSERT/UPDATE/DELETE ops/s forever.
5. `benchmark` waits for 20k orders, runs 4 analytical queries on **both**
   engines with `EXPLAIN ANALYZE` / `EXPLAIN`, writes
   `results/benchmark_results.json` + `.md` (also printed to logs).
6. `dashboard` (Streamlit) serves live ClickHouse analytics.

## Endpoints

| Service    | URL / port                          |
|------------|-------------------------------------|
| Postgres   | `localhost:5432` (postgres/postgres, db `shop`) |
| ClickHouse | HTTP `localhost:8123`, native `localhost:9000` |
| Dashboard  | http://localhost:8501               |

## Verify CDC latency (Part 3)

```bash
# 1. Insert a customer in Postgres
docker exec -it pg-oltp psql -U postgres -d shop -c \
  "INSERT INTO customers(full_name,email,city) VALUES ('Ada Test','ada@test.com','Bogota') RETURNING customer_id;"
# 2. Within ~1-2 s it appears in ClickHouse:
docker exec -it ch-olap clickhouse-client -q \
  "SELECT * FROM shop.customers_ch FINAL WHERE email='ada@test.com'"
# 3. Update + delete propagate as new versions / tombstones:
docker exec -it pg-oltp psql -U postgres -d shop -c \
  "UPDATE customers SET city='Medellin' WHERE email='ada@test.com'"
docker exec -it ch-olap clickhouse-client -q \
  "SELECT full_name, city FROM shop.customers_ch FINAL WHERE email='ada@test.com'"
```

## Benchmark (Part 4)

```bash
docker logs benchmark
cat results/benchmark_results.md
```

Queries (see `services/benchmark/benchmark.py`): revenue by
category/month, top-10 customers, AOV by city/status, daily volume.
Plans + timings + row counts are stored for the report.

## Repo layout

```
docker-compose.yml
init-db/01-schema.sql          # PG 3NF schema + outbox + triggers + publication
clickhouse/init.sql            # CH ReplacingMergeTree tables
services/cdc/worker.py         # snapshot + streaming CDC applier
services/generator/generator.py# seed + continuous I/U/D load
services/benchmark/benchmark.py# PG vs CH queries + plans + timings
services/dashboard/app.py      # Streamlit live dashboard
report/REPORT.md               # full report (ER, CDC, benchmarks)
```

## CDC design note

The assignment recommends PeerDB. PeerDB needs a separate control-plane
(Temporal, catalog DB, UI clicks / API calls to create peers & mirrors),
which breaks the "single `docker compose up`, no manual steps" requirement
and is fragile in grading environments. This project therefore ships an
equivalent open-source CDC pipeline with identical semantics:

- Postgres configured for **logical replication**
  (`wal_level=logical`, `PUBLICATION cdc_pub`, `REPLICA IDENTITY FULL`)
  so any PeerDB/Debezium mirror can attach later;
- durable change feed via `cdc_outbox` + triggers (trigger-based CDC is a
  standard CDC category), giving snapshot + streaming, ordered, idempotent
  delivery with tombstones — the same guarantees PeerDB provides over the WAL;
- ClickHouse `ReplacingMergeTree(_version)` + `FINAL` + `is_deleted`,
  exactly the PeerDB/ClickHouse mapping pattern.

See `report/REPORT.md` for the full rationale and swap-in instructions.
