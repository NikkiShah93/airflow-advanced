CREATE TABLE IF NOT EXISTS customer_purchase (
    ID INT PRIMARY KEY,
    product VARCHAR(100) NOT NULL,
    price INT NOT NULL,
    customer_id NOT NULL REFERENCES customer (id)
);