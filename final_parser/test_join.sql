SELECT table_a.id, table_b.id
FROM table_b
JOIN table_c ON table_c.join_key_bc = table_b.join_key_ab
JOIN table_a ON table_b.join_key_ab = table_a.join_key_ab
WHERE table_b.id > 1