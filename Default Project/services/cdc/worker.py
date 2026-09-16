"""CDC worker: snapshot + streaming outbox applier (Postgres -> ClickHouse).

Design (documented in report as lightweight PeerDB alternative):
  1. Ensure ClickHouse target tables exist (ReplacingMergeTree + is_deleted).
  2. One-time snapshot copy of all rows (like PeerDB initial load).
  3. Stream cdc_outbox rows (written by PG triggers = logical change feed),
     batched, idempotent, with tombstones for DELETEs.
Queries must use FINAL + is_deleted=0 to read the latest state.
"""
import json
import os
import time
from datetime import datetime, timezone

import clickhouse_connect
import psycopg2
import psycopg2.extras

PG = dict(host=os.getenv("PGHOST", "postgres"), port=int(os.getenv("PGPORT", "5432")),
          dbname=os.getenv("PGDATABASE", "shop"), user=os.getenv("PGUSER", "postgres"),
          password=os.getenv("PGPASSWORD", "postgres"))
CH = dict(host=os.getenv("CH_HOST", "clickhouse"), port=int(os.getenv("CH_PORT", "8123")),
          database=os.getenv("CH_DATABASE", "shop"),
          username=os.getenv("CH_USER", "default"), password=os.getenv("CH_PASSWORD", ""))
POLL_S = float(os.getenv("POLL_MS", "500")) / 1000.0
BATCH = int(os.getenv("BATCH_SIZE", "2000"))
SNAPSHOT = os.getenv("SNAPSHOT_ON_START", "1") == "1"

DDL = {
    "customers_ch": """CREATE TABLE IF NOT EXISTS shop.customers_ch (
      customer_id UInt32, full_name String, email String, city String,
      created_at DateTime, updated_at DateTime,
      is_deleted UInt8 DEFAULT 0, _version DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(_version) ORDER BY customer_id""",
    "products_ch": """CREATE TABLE IF NOT EXISTS shop.products_ch (
      product_id UInt32, sku String, name String, category String,
      price Decimal(10,2), stock Int32, updated_at DateTime,
      is_deleted UInt8 DEFAULT 0, _version DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(_version) ORDER BY product_id""",
    "orders_ch": """CREATE TABLE IF NOT EXISTS shop.orders_ch (
      order_id UInt32, customer_id UInt32, status String, total Decimal(12,2),
      created_at DateTime, updated_at DateTime,
      is_deleted UInt8 DEFAULT 0, _version DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(_version) ORDER BY order_id""",
    "order_items_ch": """CREATE TABLE IF NOT EXISTS shop.order_items_ch (
      item_id UInt32, order_id UInt32, product_id UInt32, qty UInt32,
      unit_price Decimal(10,2),
      is_deleted UInt8 DEFAULT 0, _version DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(_version) ORDER BY item_id""",
}

TABLES = {
    "customers": ("customers_ch",
                  ["customer_id", "full_name", "email", "city", "created_at", "updated_at"]),
    "products": ("products_ch",
                 ["product_id", "sku", "name", "category", "price", "stock", "updated_at"]),
    "orders": ("orders_ch",
               ["order_id", "customer_id", "status", "total", "created_at", "updated_at"]),
    "order_items": ("order_items_ch",
                    ["item_id", "order_id", "product_id", "qty", "unit_price"]),
}


def wait_pg():
    while True:
        try:
            c = psycopg2.connect(**PG)
            c.autocommit = True
            c.close()
            return
        except Exception as e:
            print(f"[cdc] waiting postgres: {e}", flush=True)
            time.sleep(2)


def wait_ch():
    while True:
        try:
            cl = clickhouse_connect.get_client(**CH)
            cl.query("SELECT 1")
            return cl
        except Exception as e:
            print(f"[cdc] waiting clickhouse: {e}", flush=True)
            time.sleep(2)


def parse_ts(v):
    if v is None:
        return datetime(2020, 1, 1)
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    s = str(v).replace("T", " ").split("+")[0].split(".")[0]
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime(2020, 1, 1)


def row_from_payload(tbl, payload, op):
    cols, vals, _ = TABLES[tbl][1], [], TABLES[tbl][0]
    out = []
    for c in cols:
        v = payload.get(c)
        if c in ("created_at", "updated_at"):
            out.append(parse_ts(v))
        elif c in ("price", "total", "unit_price"):
            out.append(float(v or 0))
        elif c in ("customer_id", "product_id", "order_id", "item_id",
                   "qty", "stock"):
            out.append(int(float(v or 0)))
        else:
            out.append(str(v) if v is not None else "")
    out.append(1 if op == "DELETE" else 0)          # is_deleted
    out.append(datetime.now(timezone.utc).replace(tzinfo=None))  # _version
    return out


def ch_insert(cl, tbl, rows):
    ch_table, cols = TABLES[tbl]
    full_cols = cols + ["is_deleted", "_version"]
    cl.insert(f"shop.{ch_table}", rows, column_names=full_cols)


def snapshot(pg, cl):
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for tbl, (ch_table, cols) in TABLES.items():
        cur.execute(f"SELECT * FROM {tbl}")
        batch, n = [], 0
        for r in cur.fetchall():
            batch.append(row_from_payload(tbl, dict(r), "INSERT"))
            if len(batch) >= BATCH:
                ch_insert(cl, tbl, batch)
                n += len(batch)
                batch = []
        if batch:
            ch_insert(cl, tbl, batch)
            n += len(batch)
        print(f"[cdc] snapshot {tbl}: {n} rows -> shop.{ch_table}", flush=True)


def stream(pg, cl, start_from=None):
    pg.autocommit = True
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if start_from is None:
        cur.execute("SELECT COALESCE(MAX(seq),0) AS s FROM cdc_outbox")
        start_from = cur.fetchone()["s"]
    last = start_from
    while True:
        cur.execute(
            "SELECT seq, tbl, op, payload FROM cdc_outbox "
            "WHERE seq > %s ORDER BY seq LIMIT %s", (last, BATCH))
        rows = cur.fetchall()
        if not rows:
            time.sleep(POLL_S)
            continue
        by_table = {}
        for r in rows:
            tbl = r["tbl"]
            if tbl not in TABLES:
                last = r["seq"]
                continue
            payload = r["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            by_table.setdefault(tbl, []).append(
                row_from_payload(tbl, payload, r["op"]))
            last = r["seq"]
        for tbl, batch in by_table.items():
            ch_insert(cl, tbl, batch)
        print(f"[cdc] applied {len(rows)} events, last_seq={last}", flush=True)


def main():
    wait_pg()
    cl = wait_ch()
    for ddl in DDL.values():
        cl.command(ddl)
    pg = psycopg2.connect(**PG)
    pg.autocommit = True
    start_seq = 0
    if SNAPSHOT:
        # Wait until schema exists (init script may still run)
        for _ in range(60):
            try:
                c = pg.cursor()
                c.execute("SELECT 1 FROM customers LIMIT 1")
                break
            except Exception:
                pg.rollback()
                time.sleep(2)
        # Capture the log position BEFORE the snapshot so concurrent writes
        # during the copy are replayed (idempotently) afterwards — no gaps.
        tmp = pg.cursor()
        tmp.execute("SELECT COALESCE(MAX(seq),0) AS s FROM cdc_outbox")
        start_seq = tmp.fetchone()["s"]
        snapshot(pg, cl)
    stream(pg, cl, start_from=start_seq)


if __name__ == "__main__":
    main()
