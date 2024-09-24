CREATE TABLE IF NOT EXISTS customer_purchases (
    ID INT PRIMARY KEY,
    product VARCHAR(100) NOT NULL,
    price INT NOT NULL,
    customer_id INT NOT NULL REFERENCES customers (id)
);