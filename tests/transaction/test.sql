CREATE TABLE tests_transaction_t(val vector(3));

INSERT INTO tests_transaction_t (val) SELECT ARRAY[random(), random(), random()]::real[] FROM generate_series(1, 1000);

BEGIN;
CREATE INDEX ON t USING vectors (val vector_l2_ops);
ABORT;
