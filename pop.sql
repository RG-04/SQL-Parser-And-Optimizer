-- Drop tables if they already exist
DROP TABLE IF EXISTS A, B, C;

-- Create tables
CREATE TABLE A (
    a_id INT PRIMARY KEY,
    a_val TEXT
);

CREATE TABLE B (
    b_id INT PRIMARY KEY,
    a_id INT,
    b_val TEXT
);

CREATE TABLE C (
    c_id INT PRIMARY KEY,
    b_id INT,
    c_val TEXT
);

-- Populate table A with 1000 rows
INSERT INTO A (a_id, a_val)
SELECT i, 'value_' || i
FROM generate_series(1, 1000) AS i;

-- Populate table B with 500 rows
-- Each b.a_id refers to a.a_id randomly
INSERT INTO B (b_id, a_id, b_val)
SELECT i, (random() * 999 + 1)::INT, 'bval_' || i
FROM generate_series(1, 500) AS i;

-- Populate table C with 2000 rows
-- Each c.b_id refers to b.b_id randomly
INSERT INTO C (c_id, b_id, c_val)
SELECT i, (random() * 499 + 1)::INT, 'cval_' || i
FROM generate_series(1, 2000) AS i;

-- Create indexes to simulate realistic stats
CREATE INDEX idx_b_a_id ON B(a_id);
CREATE INDEX idx_c_b_id ON C(b_id);

-- ANALYZE tables to update PostgreSQL statistics
ANALYZE;
