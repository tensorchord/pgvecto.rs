CREATE CAST (real[] AS vector)
    WITH FUNCTION cast_array_to_vector(real[], integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
    WITH FUNCTION cast_vector_to_array(vector, integer, boolean) AS IMPLICIT;

CREATE OPERATOR CLASS l2_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS dot_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS cosine_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops;

CREATE VIEW pg_vector_index_info AS
    SELECT
        C.oid AS tablerelid,
        I.oid AS indexrelid,
        C.relname AS tablename,
        I.relname AS indexname,
        vector_stat_indexing(I.oid) AS idx_indexing,
        vector_stat_tuples(I.oid) AS idx_tuples,
        vector_stat_tuples_done(I.oid) AS idx_tuples_done,
        vector_stat_config(I.oid) AS idx_config
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid JOIN
         pg_am A ON A.oid = I.relam
    WHERE A.amname = 'vectors';