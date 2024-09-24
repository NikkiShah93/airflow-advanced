CREATE TABLE IF NOT EXISTS customer_full_details AS
SELECT 
    c.id as customer_id,
    c.name as customer_name,
    cp.product as product,
    cp.price as price
FROM customers AS c LEFT JOIN customer_purchases AS cp 
ON c.id = cp.customer_id;