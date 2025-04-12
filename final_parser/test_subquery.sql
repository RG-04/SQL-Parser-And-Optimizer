SELECT a.id, b.id, c.id
FROM table_a a
JOIN (SELECT b.id, b.join_key_ab FROM table_b b WHERE b.value = 'common_value_1') tmp1 
  ON a.join_key_ab = tmp1.join_key_ab
JOIN (SELECT b.id, b.join_key_ab FROM table_b b WHERE b.value = 'common_value_1') tmp2
  ON a.join_key_ab = tmp2.join_key_ab
JOIN table_c c
  ON a.join_key_ac = c.join_key_ac
WHERE a.id > 100;