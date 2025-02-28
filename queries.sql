#################################################################################
-- Create the database
#################################################################################
CREATE DATABASE {};


#################################################################################
-- Select the database to use
#################################################################################
USE {};


#################################################################################
-- Create the clients table
#################################################################################
CREATE TABLE IF NOT EXISTS clients
(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);


#################################################################################
-- Create the sales representatives table
#################################################################################
CREATE TABLE IF NOT EXISTS sales_representatives
(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);


#################################################################################
-- Create the stores table
#################################################################################
CREATE TABLE IF NOT EXISTS stores
(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);


#################################################################################
-- Create the products table
#################################################################################
CREATE TABLE IF NOT EXISTS products
(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    store_id INT NOT NULL,
    FOREIGN KEY (store_id) REFERENCES stores(id),
    UNIQUE KEY product_store_idx (store_id, name)
);


#################################################################################
-- Create the transactions table
#################################################################################
CREATE TABLE IF NOT EXISTS transactions
(
    id VARCHAR(128) PRIMARY KEY,
    date DATE NOT NULL,
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    client_id INT NOT NULL,
    sales_repr_id INT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(id),
    FOREIGN KEY (store_id) REFERENCES stores(id),
    FOREIGN KEY (client_id) REFERENCES clients(id),
    FOREIGN KEY (sales_repr_id) REFERENCES sales_representatives(id)
);


#################################################################################
-- Create a temporary transactions table
#################################################################################
CREATE TABLE IF NOT EXISTS transactions_temp (
    transaction_id VARCHAR(128) PRIMARY KEY,
    transaction_date DATE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    store_name VARCHAR(255) NOT NULL,
    sales_representative_name VARCHAR(255) NOT NULL,
    client_name VARCHAR(255) NOT NULL
);


#################################################################################
-- Insert into the staging transactions table
#################################################################################
INSERT INTO transactions_temp
VALUES (%s, %s, %s, %s, %s, %s, %s);


#################################################################################
-- Update staging table for data consistency
#################################################################################
UPDATE transactions_temp
SET price = 4.50
WHERE product_name = 'cappuccino';


#################################################################################
-- insert into stores from staging
#################################################################################
INSERT IGNORE INTO stores (name)
SELECT DISTINCT t.store_name
FROM transactions_temp t
WHERE NOT EXISTS (SELECT 1
                  FROM stores s
                  WHERE s.name = t.store_name);


#################################################################################
-- insert into sales_representatives from staging
#################################################################################
INSERT IGNORE INTO sales_representatives (name)
SELECT DISTINCT t.sales_representative_name
FROM transactions_temp t
WHERE NOT EXISTS (SELECT 1
                  FROM stores s
                  WHERE s.name = t.sales_representative_name);


#################################################################################
-- insert into clients from staging
#################################################################################
INSERT IGNORE INTO clients (name)
SELECT DISTINCT t.client_name
FROM transactions_temp t
WHERE NOT EXISTS(SELECT 1
                 FROM clients c
                 WHERE c.name = t.client_name);


#################################################################################
-- insert into products from staging
#################################################################################
INSERT IGNORE INTO products (name, price, store_id)
SELECT DISTINCT t.product_name, t.price, s.id
FROM transactions_temp t
         JOIN stores s ON t.store_name = s.name
WHERE NOT EXISTS (SELECT 1
                  FROM products p
                  WHERE p.name = t.product_name);


#################################################################################
-- insert into transactions from staging
#################################################################################
INSERT IGNORE INTO transactions
SELECT t.transaction_id,
       t.transaction_date,
       p.id  AS product_id,
       s.id  AS store_id,
       c.id  AS client_id,
       sr.id AS sales_repr_id
FROM transactions_temp t
         JOIN stores s ON t.store_name = s.name
         JOIN products p ON t.product_name = p.name AND p.store_id = s.id
         JOIN clients c ON t.client_name = c.name
         JOIN sales_representatives sr ON t.sales_representative_name = sr.name;


#################################################################################
#    get_customers
#################################################################################
SELECT
    c.id   as client_id,
    c.name as client_name,
    t.date as transaction_date
FROM clients c
    JOIN transactions t ON t.client_id = c.id
    JOIN stores s ON s.id = t.store_id
    JOIN products p ON p.id = t.product_id AND p.store_id = s.id
WHERE s.name = 'King St'
    AND p.name = 'cappuccino'
    AND t.date =
      (SELECT MAX(date)
       FROM transactions t2
       WHERE t2.client_id = t.client_id
         AND t2.store_id = t.store_id)
;


#################################################################################
#    get_customers_sort
#################################################################################
WITH total_spent AS
    (SELECT t.client_id,
            SUM(p.price) AS total_spent
     FROM transactions t
         JOIN products p ON t.product_id = p.id
     GROUP BY t.client_id
     )
SELECT
    c.id   as client_id,
    c.name as client_name,
    t.date as transaction_date,
    ts.total_spent as total_spent
FROM clients c
    JOIN total_spent ts ON ts.client_id = c.id
    JOIN transactions t ON t.client_id = c.id
    JOIN stores s ON s.id = t.store_id
    JOIN products p ON p.id = t.product_id AND p.store_id = s.id
WHERE s.name = 'King St'
    AND p.name = 'cappuccino'
    AND t.date =
      (SELECT MAX(date)
       FROM transactions t2
       WHERE t2.client_id = t.client_id
         AND t2.store_id = t.store_id)
ORDER BY total_spent DESC
;


#################################################################################
#    get_customers_sort_optim
#################################################################################
WITH latest_trans AS
    (SELECT t.id,
            t.client_id,
            t.store_id,
            t.product_id,
            t.date,
            p.name AS product_name,
            SUM(p.price) OVER (PARTITION BY t.client_id) as total_spent,
            ROW_NUMBER() OVER (PARTITION BY t.client_id, t.store_id ORDER BY t.date DESC) AS rn
     FROM transactions t
     JOIN products p ON t.product_id = p.id)

SELECT c.id,
       c.name,
       lt.date,
       lt.total_spent as total_spent
FROM latest_trans lt
    JOIN clients c ON lt.client_id = c.id
    JOIN stores s ON lt.store_id = s.id
WHERE lt.rn = 1
  AND s.name = 'King St'
  AND lt.product_name = 'cappuccino'
ORDER BY total_spent DESC;
