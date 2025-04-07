SELECT customers.id, customers.name, orders.order_id, orders.amount
FROM customers JOIN orders AS o ON customers.id = o.customer_id
JOIN temp ON temp.id = customers.id