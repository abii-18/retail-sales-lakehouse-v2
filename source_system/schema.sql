
---Retail Sales Lakehouse - Source OLTP Schema (PostgreSQL)

-- It is OLTP-shaped, not a warehouse model
--DIM SOURCE: customers

CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(50)  NOT NULL,
    last_name       VARCHAR(50)  NOT NULL,
    email           VARCHAR(150),         
    phone           VARCHAR(20),           
    address         VARCHAR(200),
    city            VARCHAR(100),
    state           VARCHAR(50),           
    country         VARCHAR(50) DEFAULT 'India',
    signup_date     DATE NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);


--DIM SOURCE: products

CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(150) NOT NULL,
    category        VARCHAR(80)  NOT NULL,
    sub_category    VARCHAR(80),
    unit_price      NUMERIC(10,2) NOT NULL,   
    unit_cost       NUMERIC(10,2) NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

--DIM SOURCE: stores

CREATE TABLE stores (
    store_id        SERIAL PRIMARY KEY,
    store_name      VARCHAR(100) NOT NULL,
    store_type      VARCHAR(20)  NOT NULL,   
    store_size      VARCHAR(10)  NOT NULL,   
    region          VARCHAR(80),
    country         VARCHAR(50) DEFAULT 'India',
    opened_date     DATE,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);


-- FACT SOURCE: orders 

CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    store_id        INTEGER NOT NULL REFERENCES stores(store_id),
    order_date      TIMESTAMP NOT NULL,
    order_status    VARCHAR(20) NOT NULL,  
    currency_code   VARCHAR(3)  NOT NULL,   
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);


--FACT SOURCE: order_items 

CREATE TABLE order_items (
    order_item_id       SERIAL PRIMARY KEY,
    order_id            INTEGER NOT NULL REFERENCES orders(order_id),
    product_id          INTEGER NOT NULL REFERENCES products(product_id),
    quantity            INTEGER NOT NULL,
    unit_price_at_sale  NUMERIC(10,2) NOT NULL, 
    unit_cost_at_sale   NUMERIC(10,2) NOT NULL, 
    discount_amount     NUMERIC(10,2) DEFAULT 0,
    line_total          NUMERIC(12,2) NOT NULL, 
    created_at          TIMESTAMP NOT NULL DEFAULT now()
);


--SOURCE: payments

CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    payment_method  VARCHAR(30) NOT NULL,   
    payment_status  VARCHAR(20) NOT NULL,   
    currency_code   VARCHAR(3)  NOT NULL,
    payment_date    TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT now()
);


--  indexes for incremental extraction (watermark queries)

CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_payments_order ON payments(order_id);
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_products_updated_at ON products(updated_at);
