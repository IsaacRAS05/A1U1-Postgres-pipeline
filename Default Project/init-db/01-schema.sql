-- ============================================================
-- U1T01: E-commerce OLTP schema (3NF) + trigger-based CDC outbox
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
  customer_id SERIAL PRIMARY KEY,
  full_name   TEXT NOT NULL,
  email       TEXT NOT NULL UNIQUE,
  city        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
  product_id SERIAL PRIMARY KEY,
  sku        TEXT NOT NULL UNIQUE,
  name       TEXT NOT NULL,
  category   TEXT NOT NULL,
  price      NUMERIC(10,2) NOT NULL CHECK (price >= 0),
  stock      INTEGER NOT NULL DEFAULT 100 CHECK (stock >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  order_id    SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
  status      TEXT NOT NULL DEFAULT 'paid'
              CHECK (status IN ('created','paid','shipped','delivered','cancelled','refunded')),
  total       NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (total >= 0),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);

CREATE TABLE IF NOT EXISTS order_items (
  item_id    SERIAL PRIMARY KEY,
  order_id   INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
  product_id INTEGER NOT NULL REFERENCES products(product_id),
  qty        INTEGER NOT NULL CHECK (qty > 0),
  unit_price NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0)
);
CREATE INDEX IF NOT EXISTS idx_items_order   ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_items_product ON order_items(product_id);

-- ---------- CDC outbox (durable change log) ----------
CREATE TABLE IF NOT EXISTS cdc_outbox (
  seq        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tbl        TEXT NOT NULL,
  op         TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  pk_id      TEXT NOT NULL,
  payload    JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_outbox_seq ON cdc_outbox(seq);

-- ---------- Generic outbox trigger function ----------
CREATE OR REPLACE FUNCTION fn_cdc_outbox() RETURNS trigger AS $$
DECLARE
  j jsonb;
  v_pk text;
BEGIN
  IF TG_OP = 'DELETE' THEN
    j := row_to_json(OLD)::jsonb;
  ELSE
    j := row_to_json(NEW)::jsonb;
  END IF;
  v_pk := COALESCE(j->>'customer_id', j->>'product_id', j->>'order_id', j->>'item_id', '0');
  INSERT INTO cdc_outbox(tbl, op, pk_id, payload)
  VALUES (TG_TABLE_NAME, TG_OP, v_pk, j);
  PERFORM pg_notify('cdc_events', TG_TABLE_NAME || ':' || TG_OP || ':' || v_pk);
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cdc_customers ON customers;
CREATE TRIGGER trg_cdc_customers AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW EXECUTE FUNCTION fn_cdc_outbox();

DROP TRIGGER IF EXISTS trg_cdc_products ON products;
CREATE TRIGGER trg_cdc_products AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW EXECUTE FUNCTION fn_cdc_outbox();

DROP TRIGGER IF EXISTS trg_cdc_orders ON orders;
CREATE TRIGGER trg_cdc_orders AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION fn_cdc_outbox();

DROP TRIGGER IF EXISTS trg_cdc_order_items ON order_items;
CREATE TRIGGER trg_cdc_order_items AFTER INSERT OR UPDATE OR DELETE ON order_items
FOR EACH ROW EXECUTE FUNCTION fn_cdc_outbox();

-- ---------- Logical replication (PeerDB-style) ----------
-- Publication so the setup is logically-replication ready
-- (our bundled cdc-worker uses the durable outbox; any
-- Debezium/PeerDB mirror can attach to this publication too).
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'cdc_pub') THEN
    CREATE PUBLICATION cdc_pub FOR TABLE customers, products, orders, order_items;
  END IF;
END
$$;
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
