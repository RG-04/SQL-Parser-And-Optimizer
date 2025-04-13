SELECT table_a.id, table_b.id
FROM table_a
JOIN table_b ON table_b.join_key_ab = table_a.join_key_ab
JOIN table_c ON table_c.join_key_bc = table_b.join_key_ab
WHERE table_b.id > 1