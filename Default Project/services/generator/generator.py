"""Continuous OLTP load generator: INSERT / UPDATE / DELETE against Postgres."""
import os
import random
import time

import psycopg2
import psycopg2.extras
from faker import Faker

PG = dict(host=os.getenv("PGHOST", "postgres"), port=int(os.getenv("PGPORT", "5432")),
          dbname=os.getenv("PGDATABASE", "shop"), user=os.getenv("PGUSER", "postgres"),
          password=os.getenv("PGPASSWORD", "postgres"))
SEED_CUSTOMERS = int(os.getenv("SEED_CUSTOMERS", "5000"))
SEED_PRODUCTS = int(os.getenv("SEED_PRODUCTS", "500"))
SEED_ORDERS = int(os.getenv("SEED_ORDERS", "30000"))
OPS_PER_SEC = float(os.getenv("OPS_PER_SEC", "120"))
UPDATE_RATIO = float(os.getenv("UPDATE_RATIO", "0.35"))
DELETE_RATIO = float(os.getenv("DELETE_RATIO", "0.05"))

CITIES = ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena", "Lima",
          "Santiago", "CDMX", "Buenos Aires", "Sao Paulo"]
CATS = ["electronics", "fashion", "grocery", "toys", "sports", "beauty", "home"]
STATUSES = ["created", "paid", "paid", "shipped", "delivered", "delivered",
            "cancelled", "refunded"]
fake = Faker()
Faker.seed(7)
random.seed(7)


def conn():
    while True:
        try:
            c = psycopg2.connect(**PG)
            c.autocommit = True
            return c
        except Exception as e:
            print(f"[gen] waiting pg: {e}", flush=True)
            time.sleep(2)


def seed(cur):
    cur.execute("SELECT COUNT(*) FROM customers")
    if cur.fetchone()[0] >= SEED_CUSTOMERS:
        print("[gen] seed already present, skipping", flush=True)
        return
    print(f"[gen] seeding {SEED_CUSTOMERS} customers, {SEED_PRODUCTS} products, "
          f"{SEED_ORDERS} orders...", flush=True)
    for i in range(0, SEED_CUSTOMERS, 1000):
        rows = [(fake.name(), f"user{i+j}_{random.randint(0,999999)}@mail.com",
                 random.choice(CITIES)) for j in range(min(1000, SEED_CUSTOMERS - i))]
        psycopg2.extras.execute_values(
            cur, "INSERT INTO customers(full_name,email,city) VALUES %s "
                 "ON CONFLICT (email) DO NOTHING", rows)
    for i in range(SEED_PRODUCTS):
        cur.execute(
            "INSERT INTO products(sku,name,category,price,stock) VALUES (%s,%s,%s,%s,%s) "
            "ON CONFLICT (sku) DO NOTHING",
            (f"SKU-{i:06d}", fake.word().capitalize() + f" {i}",
             random.choice(CATS), round(random.uniform(5, 2000), 2),
             random.randint(10, 500)))
    cur.execute("SELECT customer_id FROM customers")
    custs = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, price FROM products")
    prods = cur.fetchall()
    for _ in range(SEED_ORDERS):
        c = random.choice(custs)
        cur.execute(
            "INSERT INTO orders(customer_id,status,total) VALUES (%s,%s,0) RETURNING order_id",
            (c, random.choice(STATUSES)))
        oid = cur.fetchone()[0]
        total, n = 0, random.randint(1, 5)
        for _ in range(n):
            pid, price = random.choice(prods)
            q = random.randint(1, 4)
            cur.execute(
                "INSERT INTO order_items(order_id,product_id,qty,unit_price) "
                "VALUES (%s,%s,%s,%s)", (oid, pid, q, price))
            total += float(price) * q
        cur.execute("UPDATE orders SET total=%s WHERE order_id=%s", (total, oid))
    print("[gen] seed done", flush=True)


def hot_loop(cur):
    import psycopg2.extras
    ops = 0
    t0 = time.time()
    while True:
        r = random.random()
        try:
            if r < DELETE_RATIO:
                # delete a delivered/cancelled order item batch (tombstone path in CH)
                cur.execute(
                    "DELETE FROM order_items WHERE item_id IN "
                    "(SELECT item_id FROM order_items ORDER BY random() LIMIT 3)")
            elif r < DELETE_RATIO + UPDATE_RATIO:
                what = random.random()
                if what < 0.4:
                    cur.execute(
                        "UPDATE products SET stock = stock - %s, updated_at=now() "
                        "WHERE product_id=(SELECT product_id FROM products "
                        "ORDER BY random() LIMIT 1) AND stock >= %s",
                        (random.randint(1, 3), random.randint(1, 3)))
                elif what < 0.7:
                    cur.execute(
                        "UPDATE orders SET status=%s, updated_at=now() "
                        "WHERE order_id=(SELECT order_id FROM orders "
                        "ORDER BY random() LIMIT 1)",
                        (random.choice(STATUSES),))
                else:
                    cur.execute(
                        "UPDATE customers SET city=%s, updated_at=now() "
                        "WHERE customer_id=(SELECT customer_id FROM customers "
                        "ORDER BY random() LIMIT 1)",
                        (random.choice(CITIES),))
            else:
                cur.execute(
                    "SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
                c = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO orders(customer_id,status,total) VALUES "
                    "(%s,'created',0) RETURNING order_id", (c,))
                oid = cur.fetchone()[0]
                total = 0
                for _ in range(random.randint(1, 4)):
                    cur.execute(
                        "SELECT product_id, price FROM products "
                        "ORDER BY random() LIMIT 1")
                    pid, price = cur.fetchone()
                    q = random.randint(1, 3)
                    cur.execute(
                        "INSERT INTO order_items(order_id,product_id,qty,unit_price)"
                        " VALUES (%s,%s,%s,%s)", (oid, pid, q, price))
                    total += float(price) * q
                cur.execute(
                    "UPDATE orders SET status='paid', total=%s, updated_at=now() "
                    "WHERE order_id=%s", (total, oid))
        except Exception as e:
            print(f"[gen] op error: {e}", flush=True)
        ops += 1
        if ops % 500 == 0:
            el = time.time() - t0
            print(f"[gen] {ops} ops in {el:.0f}s ({ops/el:.1f} ops/s)", flush=True)
        time.sleep(1.0 / OPS_PER_SEC)


if __name__ == "__main__":
    import psycopg2.extras
    c = conn()
    cur = c.cursor()
    # wait for schema
    for _ in range(60):
        try:
            cur.execute("SELECT 1 FROM customers LIMIT 1")
            break
        except Exception:
            c.rollback()
            time.sleep(2)
    seed(cur)
    hot_loop(cur)
