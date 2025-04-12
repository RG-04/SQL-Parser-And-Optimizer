-- Create more extreme data distributions
DROP DATABASE IF EXISTS temp;
CREATE DATABASE temp;

\c temp

CREATE TABLE table_a (
    id INTEGER PRIMARY KEY,
    value VARCHAR(50),
    join_key_ab INTEGER,
    join_key_ac INTEGER
);

CREATE TABLE table_b (
    id INTEGER PRIMARY KEY,
    value VARCHAR(50),
    join_key_ab INTEGER,
    join_key_bc INTEGER
);

CREATE TABLE table_c (
    id INTEGER PRIMARY KEY,
    value VARCHAR(50),
    join_key_ac INTEGER,
    join_key_bc INTEGER
);

-- Table A: Medium size (5000 rows)
INSERT INTO table_a (id, value, join_key_ab, join_key_ac)
SELECT 
    generate_series(1, 5000),
    'value_a_' || generate_series(1, 5000),
    (random() * 100)::int + 1,  -- Small range of join keys with table B
    (random() * 1000)::int + 1; -- Larger range of join keys with table C

-- Table B: Small size (1000 rows)
INSERT INTO table_b (id, value, join_key_ab, join_key_bc)
SELECT 
    generate_series(1, 1000),
    'value_b_' || generate_series(1, 1000),
    (random() * 100)::int + 1,  -- Small range of join keys with table A
    (random() * 500)::int + 1;  -- Medium range of join keys with table C

-- Table C: Large size (10000 rows)
INSERT INTO table_c (id, value, join_key_ac, join_key_bc)
SELECT 
    generate_series(1, 10000),
    'value_c_' || generate_series(1, 10000),
    (random() * 1000)::int + 1, -- Larger range of join keys with table A
    (random() * 500)::int + 1;  -- Medium range of join keys with table B

-- Create skewed distributions for MCV
UPDATE table_a SET value = 'common_value_1' WHERE id % 10 = 0; -- 10% of rows
UPDATE table_b SET value = 'common_value_1' WHERE id % 5 = 0;  -- 20% of rows
UPDATE table_c SET value = 'common_value_1' WHERE id % 20 = 0; -- 5% of rows

-- Create indexes
CREATE INDEX idx_a_join_key_ab ON table_a(join_key_ab);
CREATE INDEX idx_a_join_key_ac ON table_a(join_key_ac);
CREATE INDEX idx_b_join_key_ab ON table_b(join_key_ab);
CREATE INDEX idx_b_join_key_bc ON table_b(join_key_bc);
CREATE INDEX idx_c_join_key_ac ON table_c(join_key_ac);
CREATE INDEX idx_c_join_key_bc ON table_c(join_key_bc);

-- Analyze tables
ANALYZE table_a;
ANALYZE table_b;
ANALYZE table_c;