#!/bin/bash
cd "$(dirname "$0")"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  DataIQ Platform — Seeding Demo Schema & Tables          "
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

python3 << 'PYEOF'
import sys
try:
    import psycopg2
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q", "--break-system-packages"])
    import psycopg2

print("📡 Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host='localhost', port=5432, database='postgres',
        user='milan', password='admin'
    )
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("   Make sure PostgreSQL is running and credentials are correct.")
    sys.exit(1)

conn.autocommit = True
cur = conn.cursor()

print("🗂️  Creating demo schema...")
cur.execute("CREATE SCHEMA IF NOT EXISTS demo;")

print("📋 Creating tables...")

cur.execute("""
CREATE TABLE IF NOT EXISTS demo.customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    phone           VARCHAR(20),
    date_of_birth   DATE,
    country         VARCHAR(100) DEFAULT 'USA',
    city            VARCHAR(100),
    signup_date     TIMESTAMP DEFAULT NOW(),
    is_active       BOOLEAN DEFAULT TRUE,
    lifetime_value  NUMERIC(12,2) DEFAULT 0.00
);""")
print("   ✅ demo.customers")

cur.execute("""
CREATE TABLE IF NOT EXISTS demo.products (
    product_id      SERIAL PRIMARY KEY,
    sku             VARCHAR(50) UNIQUE NOT NULL,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    price           NUMERIC(10,2) NOT NULL,
    cost            NUMERIC(10,2),
    stock_quantity  INTEGER DEFAULT 0,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);""")
print("   ✅ demo.products")

cur.execute("""
CREATE TABLE IF NOT EXISTS demo.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES demo.customers(customer_id),
    order_date      TIMESTAMP DEFAULT NOW(),
    status          VARCHAR(50) DEFAULT 'pending',
    total_amount    NUMERIC(12,2),
    currency        VARCHAR(3) DEFAULT 'USD',
    shipping_address TEXT,
    payment_method  VARCHAR(50)
);""")
print("   ✅ demo.orders")

cur.execute("""
CREATE TABLE IF NOT EXISTS demo.order_items (
    item_id         SERIAL PRIMARY KEY,
    order_id        INTEGER REFERENCES demo.orders(order_id),
    product_id      INTEGER REFERENCES demo.products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    discount        NUMERIC(5,2) DEFAULT 0.00
);""")
print("   ✅ demo.order_items")

cur.execute("""
CREATE TABLE IF NOT EXISTS demo.employees (
    employee_id     SERIAL PRIMARY KEY,
    name            VARCHAR(200) NOT NULL,
    email           VARCHAR(255),
    department      VARCHAR(100),
    role            VARCHAR(100),
    salary          NUMERIC(12,2),
    hire_date       DATE,
    manager_id      INTEGER,
    is_active       BOOLEAN DEFAULT TRUE
);""")
print("   ✅ demo.employees")

cur.execute("""
CREATE TABLE IF NOT EXISTS demo.events (
    event_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES demo.customers(customer_id),
    event_type      VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMP DEFAULT NOW(),
    session_id      VARCHAR(100),
    page_url        TEXT,
    device_type     VARCHAR(50),
    country_code    VARCHAR(3),
    properties      JSONB
);""")
print("   ✅ demo.events")

print("")
print("🌱 Seeding sample data...")

cur.execute("""
INSERT INTO demo.customers (first_name, last_name, email, phone, date_of_birth, city, country, lifetime_value) VALUES
  ('Alice',   'Johnson',  'alice.johnson@example.com',   '+1-555-0101',      '1990-03-15', 'New York',    'USA',     12450.00),
  ('Bob',     'Smith',    'bob.smith@example.com',       '+1-555-0102',      '1985-07-22', 'Chicago',     'USA',      8920.50),
  ('Carmen',  'Garcia',   'carmen.garcia@example.com',   '+34-91-555-0103',  '1992-11-08', 'Madrid',      'Spain',    5300.75),
  ('David',   'Lee',      'david.lee@example.com',       NULL,               '1988-01-30', 'Seoul',       'Korea',    3200.00),
  ('Emma',    'Wilson',   'emma.wilson@example.com',     '+44-20-555-0105',  '1995-05-12', 'London',      'UK',      18900.25),
  ('Frank',   'Brown',    'frank.brown@example.com',     '+1-555-0106',      NULL,         'Austin',      'USA',       NULL),
  ('Grace',   'Martinez', 'grace.martinez@example.com',  '+1-555-0107',      '1993-09-21', 'Miami',       'USA',      7650.00),
  ('Henry',   'Anderson', 'henry.anderson@example.com',  '+1-555-0108',      '1980-12-05', 'Seattle',     'USA',     22100.00),
  ('Isabel',  'Thomas',   'isabel.thomas@example.com',   '+49-30-555-0109',  '1991-04-17', 'Berlin',      'Germany',  9400.50),
  ('James',   'Taylor',   'james.taylor@example.com',    '+1-555-0110',      '1987-08-29', 'Boston',      'USA',     15200.75)
ON CONFLICT (email) DO NOTHING;
""")
print("   ✅ 10 customers")

cur.execute("""
INSERT INTO demo.products (sku, name, category, subcategory, price, cost, stock_quantity) VALUES
  ('LAPTOP-001',   'ProBook 15 Laptop',          'Electronics', 'Computers',    1299.99, 780.00,  45),
  ('PHONE-001',    'SmartX Pro Smartphone',      'Electronics', 'Phones',        899.99, 420.00, 120),
  ('TABLET-001',   'TabPad Air 10"',             'Electronics', 'Tablets',       549.99, 280.00,  60),
  ('HDPHONE-001',  'SoundMax ANC Headphones',    'Electronics', 'Audio',         249.99, 110.00, 200),
  ('CHAIR-001',    'ErgoWork Office Chair',      'Furniture',   'Seating',       449.00, 180.00,  30),
  ('DESK-001',     'StandUp Pro Desk',           'Furniture',   'Desks',         799.00, 320.00,  15),
  ('MONITOR-001',  'ViewPro 27" 4K Monitor',     'Electronics', 'Displays',      599.99, 280.00,  55),
  ('KEYBOARD-001', 'MechaType Pro Keyboard',     'Electronics', 'Accessories',   149.99,  60.00, 300),
  ('MOUSE-001',    'PrecisionClick Wireless',    'Electronics', 'Accessories',    79.99,  28.00, 500),
  ('WEBCAM-001',   'ClearView 4K Webcam',        'Electronics', 'Accessories',   119.99,  48.00, 150)
ON CONFLICT (sku) DO NOTHING;
""")
print("   ✅ 10 products")

cur.execute("""
INSERT INTO demo.orders (customer_id, order_date, status, total_amount, payment_method) VALUES
  (1, NOW() - INTERVAL '30 days', 'completed',  1549.98, 'credit_card'),
  (2, NOW() - INTERVAL '25 days', 'completed',   899.99, 'paypal'),
  (3, NOW() - INTERVAL '20 days', 'completed',   699.98, 'credit_card'),
  (5, NOW() - INTERVAL '15 days', 'shipped',    1199.98, 'credit_card'),
  (1, NOW() - INTERVAL '10 days', 'processing',  249.99, 'debit_card'),
  (8, NOW() - INTERVAL '7 days',  'completed',   599.99, 'credit_card'),
  (4, NOW() - INTERVAL '5 days',  'pending',     149.99, 'paypal'),
  (7, NOW() - INTERVAL '3 days',  'completed',  1849.99, 'credit_card'),
  (9, NOW() - INTERVAL '2 days',  'processing',  449.00, 'wire_transfer'),
  (10,NOW() - INTERVAL '1 day',   'pending',     919.98, 'credit_card');
""")
cur.execute("""
INSERT INTO demo.order_items (order_id, product_id, quantity, unit_price) VALUES
  (1,1,1,1299.99),(1,9,1,79.99),
  (2,2,1,899.99),
  (3,4,1,249.99),(3,8,1,149.99),
  (4,7,1,599.99),(4,8,1,149.99),
  (5,4,1,249.99),
  (6,7,1,599.99),
  (7,8,1,149.99),
  (8,1,1,1299.99),(8,9,1,79.99),
  (9,5,1,449.00),
  (10,2,1,899.99),(10,9,1,79.99);
""")
print("   ✅ 10 orders + 15 order items")

cur.execute("""
INSERT INTO demo.employees (name, email, department, role, salary, hire_date) VALUES
  ('Sarah Connor',   'sarah.connor@company.com',  'Engineering', 'VP Engineering',   185000, '2018-03-01'),
  ('John Reese',     'john.reese@company.com',    'Engineering', 'Senior Engineer',  145000, '2019-06-15'),
  ('Maria Santos',   'maria.santos@company.com',  'Marketing',   'CMO',              175000, '2017-11-20'),
  ('Tom Bradley',    'tom.bradley@company.com',   'Sales',       'Sales Director',   140000, '2020-02-10'),
  ('Priya Patel',    NULL,                        'Engineering', 'Data Engineer',    130000, '2021-04-05'),
  ('Alex Kim',       'alex.kim@company.com',      'HR',          'HR Manager',       110000, '2019-09-22'),
  ('Lisa Chen',      'lisa.chen@company.com',     'Finance',     'CFO',              190000, '2016-07-14'),
  ('Mark Davis',     'mark.davis@company.com',    NULL,          'Product Manager',  125000, NULL),
  ('Nina Rodriguez', 'nina.r@company.com',        'Marketing',   'Marketing Manager',115000, '2022-01-17'),
  ('Carlos Gomez',   'carlos.gomez@company.com',  'Engineering', 'Backend Engineer', 135000, '2021-08-30')
ON CONFLICT DO NOTHING;
""")
print("   ✅ 10 employees (2 with intentional nulls for data quality demo)")

cur.execute("""
INSERT INTO demo.events (customer_id, event_type, event_timestamp, session_id, page_url, device_type, country_code) VALUES
  (1, 'page_view',   NOW() - INTERVAL '2 hours',    'sess_001', '/products',   'desktop', 'US'),
  (1, 'add_to_cart', NOW() - INTERVAL '1.9 hours',  'sess_001', '/products/1', 'desktop', 'US'),
  (1, 'purchase',    NOW() - INTERVAL '1.8 hours',  'sess_001', '/checkout',   'desktop', 'US'),
  (2, 'page_view',   NOW() - INTERVAL '5 hours',    'sess_002', '/home',       'mobile',  'US'),
  (2, 'page_view',   NOW() - INTERVAL '4.9 hours',  'sess_002', '/products',   'mobile',  'US'),
  (3, 'login',       NOW() - INTERVAL '1 day',      'sess_003', '/login',      'tablet',  'ES'),
  (5, 'page_view',   NOW() - INTERVAL '3 hours',    'sess_004', '/home',       'desktop', 'GB'),
  (5, 'search',      NOW() - INTERVAL '2.9 hours',  'sess_004', '/search',     'desktop', 'GB'),
  (7, 'page_view',   NOW() - INTERVAL '30 minutes', 'sess_005', '/account',    'mobile',  'US'),
  (8, 'purchase',    NOW() - INTERVAL '6 hours',    'sess_006', '/checkout',   'desktop', 'US')
ON CONFLICT DO NOTHING;
""")
print("   ✅ 10 events")

cur.close()
conn.close()

print("")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("  ✅ Demo schema ready!                                   ")
print("  Schema: demo                                            ")
print("  Tables: customers, products, orders, order_items,      ")
print("          employees, events                              ")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
PYEOF
