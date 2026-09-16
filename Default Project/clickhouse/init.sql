CREATE DATABASE IF NOT EXISTS shop;

CREATE TABLE IF NOT EXISTS shop.customers_ch (
  customer_id UInt32,
  full_name   String,
  email       String,
  city        String,
  created_at  DateTime,
  updated_at  DateTime,
  is_deleted  UInt8 DEFAULT 0,
  _version    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_version)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS shop.products_ch (
  product_id UInt32,
  sku        String,
  name       String,
  category   String,
  price      Decimal(10, 2),
  stock      Int32,
  updated_at DateTime,
  is_deleted UInt8 DEFAULT 0,
  _version   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_version)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS shop.orders_ch (
  order_id    UInt32,
  customer_id UInt32,
  status      String,
  total       Decimal(12, 2),
  created_at  DateTime,
  updated_at  DateTime,
  is_deleted  UInt8 DEFAULT 0,
  _version    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_version)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS shop.order_items_ch (
  item_id    UInt32,
  order_id   UInt32,
  product_id UInt32,
  qty        UInt32,
  unit_price Decimal(10, 2),
  is_deleted UInt8 DEFAULT 0,
  _version   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_version)
ORDER BY item_id;
