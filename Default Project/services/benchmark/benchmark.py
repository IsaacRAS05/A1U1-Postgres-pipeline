"""Benchmark: same analytical workload on Postgres (OLTP) vs ClickHouse (OLAP).

- Waits until Postgres holds MIN_ORDERS orders (generator running).
- Runs 4 analytical queries on both engines, timing wall-clock.
- Captures EXPLAIN ANALYZE (PG) and EXPLAIN (CH) plans.
- Writes /results/benchmark_results.json and benchmark_results.md.
"""
import json
import os
import time

import clickhouse_connect
import psycopg2

PG = dict(host=os.getenv("PGHOST", "postgres"), port=int(os.getenv("PGPORT", "5432")),
          dbname=os.getenv("PGDATABASE", "shop"), user=os.getenv("PGUSER", "postgres"),
          password=os.getenv("PGPASSWORD", "postgres"))
CH = dict(host=os.getenv("CH_HOST", "clickhouse"), port=int(os.getenv("CH_PORT", "8123")),
          database=os.getenv("CH_DATABASE", "shop"),
          username=os.getenv("CH_USER", "default"), password=os.getenv("CH_PASSWORD", ""))
MIN_ORDERS = int(os.getenv("MIN_ORDERS", "20000"))
TIMEOUT = int(os.getenv("WAIT_TIMEOUT_SEC", "600"))

QUERIES = {
    "Q1_revenue_by_category_month": {
        "pg": """SELECT p.category, date_trunc('month', o.created_at) AS month,
                        SUM(oi.qty * oi.unit_price) AS revenue, COUNT(*) AS lines
                 FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
                 JOIN products p ON p.product_id = oi.product_id
                 WHERE o.status NOT IN ('cancelled','refunded')
                 GROUP BY 1, 2 ORDER BY 1, 2""",
        "ch": """SELECT p.category, toStartOfMonth(o.created_at) AS month,
                        SUM(oi.qty * oi.unit_price) AS revenue, COUNT(*) AS lines
                 FROM shop.orders_ch AS o FINAL
                 JOIN shop.order_items_ch AS oi FINAL ON oi.order_id = o.order_id
                 JOIN shop.products_ch AS p FINAL ON p.product_id = oi.product_id
                 WHERE o.is_deleted = 0 AND oi.is_deleted = 0 AND p.is_deleted = 0
                   AND o.status NOT IN ('cancelled','refunded')
                 GROUP BY 1, 2 ORDER BY 1, 2""",
    },
    "Q2_top10_customers": {
        "pg": """SELECT c.customer_id, c.full_name, c.city,
                        SUM(o.total) AS spend, COUNT(*) AS orders
                 FROM customers c JOIN orders o ON o.customer_id = c.customer_id
                 WHERE o.status IN ('paid','shipped','delivered')
                 GROUP BY 1,2,3 ORDER BY spend DESC LIMIT 10""",
        "ch": """SELECT c.customer_id, c.full_name, c.city,
                        SUM(o.total) AS spend, COUNT(*) AS orders
                 FROM shop.customers_ch AS c FINAL
                 JOIN shop.orders_ch AS o FINAL ON o.customer_id = c.customer_id
                 WHERE c.is_deleted = 0 AND o.is_deleted = 0
                   AND o.status IN ('paid','shipped','delivered')
                 GROUP BY 1,2,3 ORDER BY spend DESC LIMIT 10""",
    },
    "Q3_aov_by_city_status": {
        "pg": """SELECT c.city, o.status, COUNT(*) AS orders,
                        AVG(o.total) AS aov, SUM(o.total) AS revenue
                 FROM customers c JOIN orders o ON o.customer_id = c.customer_id
                 GROUP BY 1, 2 ORDER BY 1, 2""",
        "ch": """SELECT c.city, o.status, COUNT(*) AS orders,
                        AVG(o.total) AS aov, SUM(o.total) AS revenue
                 FROM shop.customers_ch AS c FINAL
                 JOIN shop.orders_ch AS o FINAL ON o.customer_id = c.customer_id
                 WHERE c.is_deleted = 0 AND o.is_deleted = 0
                 GROUP BY 1, 2 ORDER BY 1, 2""",
    },
    "Q4_daily_volume_low_stock": {
        "pg": """SELECT date_trunc('day', o.created_at) AS day, COUNT(*) AS orders,
                        SUM(o.total) AS revenue,
                        COUNT(*) FILTER (WHERE o.status='cancelled') AS cancelled
                 FROM orders o
                 WHERE o.created_at > now() - interval '90 days'
                 GROUP BY 1 HAVING COUNT(*) > 5 ORDER BY 1""",
        "ch": """SELECT toStartOfDay(o.created_at) AS day, COUNT(*) AS orders,
                        SUM(o.total) AS revenue,
                        countIf(o.status = 'cancelled') AS cancelled
                 FROM shop.orders_ch AS o FINAL
                 WHERE o.is_deleted = 0 AND o.created_at > now() - INTERVAL 90 DAY
                 GROUP BY 1 HAVING COUNT(*) > 5 ORDER BY 1""",
    },
}


def wait_data():
    t0 = time.time()
    while time.time() - t0 < TIMEOUT:
        try:
            c = psycopg2.connect(**PG)
            c.autocommit = True
            cur = c.cursor()
            cur.execute("SELECT COUNT(*) FROM orders")
            n = cur.fetchone()[0]
            c.close()
            print(f"[bench] postgres orders={n} (target {MIN_ORDERS})", flush=True)
            if n >= MIN_ORDERS:
                time.sleep(10)  # let CDC catch up
                return
        except Exception as e:
            print(f"[bench] waiting: {e}", flush=True)
        time.sleep(10)
    print("[bench] timeout waiting for data; running anyway", flush=True)


def run_pg(cur, sql):
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    dt = time.perf_counter() - t0
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + sql)
    plan = "\n".join(r[0] for r in cur.fetchall())
    return dt, len(rows), plan


def run_ch(cl, sql):
    plan = ""
    try:
        r = cl.query("EXPLAIN " + sql)
        plan = "\n".join(" ".join(str(x) for x in row) for row in r.result_rows)
    except Exception as e:
        plan = f"EXPLAIN failed: {e}"
    t0 = time.perf_counter()
    res = cl.query(sql)
    dt = time.perf_counter() - t0
    return dt, len(res.result_rows), plan


def counts():
    out = {}
    try:
        c = psycopg2.connect(**PG)
        cur = c.cursor()
        for t in ("customers", "products", "orders", "order_items"):
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            out[f"pg_{t}"] = cur.fetchone()[0]
        c.close()
    except Exception as e:
        out["pg_error"] = str(e)
    try:
        cl = clickhouse_connect.get_client(**CH)
        for t in ("customers_ch", "products_ch", "orders_ch", "order_items_ch"):
            r = cl.query(f"SELECT COUNT() FROM shop.{t} FINAL WHERE is_deleted = 0")
            out[f"ch_{t}"] = r.result_rows[0][0]
        cl.close()
    except Exception as e:
        out["ch_error"] = str(e)
    return out


def main():
    wait_data()
    pg = psycopg2.connect(**PG)
    pg.autocommit = True
    pcur = pg.cursor()
    cl = clickhouse_connect.get_client(**CH)
    results = {"counts": counts(), "queries": {}}
    print(f"[bench] counts: {results['counts']}", flush=True)
    for name, q in QUERIES.items():
        pg_t, pg_n, pg_plan = run_pg(pcur, q["pg"])
        ch_t, ch_n, ch_plan = run_ch(cl, q["ch"])
        results["queries"][name] = {
            "pg_time_s": round(pg_t, 3), "pg_rows": pg_n, "pg_plan": pg_plan,
            "ch_time_s": round(ch_t, 3), "ch_rows": ch_n, "ch_plan": ch_plan,
            "speedup_ch_vs_pg": round(pg_t / ch_t, 2) if ch_t > 0 else None,
        }
        print(f"[bench] {name}: PG {pg_t:.2f}s ({pg_n} rows) | "
              f"CH {ch_t:.2f}s ({ch_n} rows) | speedup {pg_t/ch_t:.1f}x",
              flush=True)
    os.makedirs("/results", exist_ok=True)
    with open("/results/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    with open("/results/benchmark_results.md", "w") as f:
        f.write("# Benchmark results (PG vs ClickHouse, generator running)\n\n")
        f.write(f"Counts: `{json.dumps(results['counts'])}`\n\n")
        f.write("| query | PG (s) | CH (s) | speedup |\n|---|---|---|---|\n")
        for n, r in results["queries"].items():
            f.write(f"| {n} | {r['pg_time_s']} | {r['ch_time_s']} | "
                    f"{r['speedup_ch_vs_pg']}x |\n")
        for n, r in results["queries"].items():
            f.write(f"\n## {n}\n\n### PG plan\n```\n{r['pg_plan'][:3000]}\n```\n"
                    f"\n### CH plan\n```\n{r['ch_plan'][:3000]}\n```\n")
    print("[bench] wrote /results/benchmark_results.{json,md}", flush=True)


if __name__ == "__main__":
    main()
