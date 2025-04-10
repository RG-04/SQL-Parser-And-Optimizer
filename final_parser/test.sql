SELECT a.id, b.id
FROM b
JOIN (SELECT a.id FROM a) tmp ON tmp.a.id = b.id
JOIN (SELECT a.id FROM a) tmp2 ON tmp2.a.id = tmp.a.id
WHERE b.id > 1