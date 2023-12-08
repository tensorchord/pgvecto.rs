CREATE CAST (real[] AS vector)
    WITH FUNCTION vector_cast_array_to_vector(real[], integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
    WITH FUNCTION vector_cast_vector_to_array(vector, integer, boolean) AS IMPLICIT;

CREATE ACCESS METHOD vectors TYPE INDEX HANDLER vectors_amhandler;
COMMENT ON ACCESS METHOD vectors IS 'pgvecto.rs index access method';

CREATE OPERATOR CLASS l2_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS dot_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS cosine_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vecf16_l2_ops
    FOR TYPE vecf16 USING vectors AS
    OPERATOR 1 <-> (vecf16, vecf16) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vecf16_dot_ops
    FOR TYPE vecf16 USING vectors AS
    OPERATOR 1 <#> (vecf16, vecf16) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vecf16_cosine_ops
    FOR TYPE vecf16 USING vectors AS
    OPERATOR 1 <=> (vecf16, vecf16) FOR ORDER BY float_ops;

CREATE VIEW pg_vector_index_info AS
    SELECT
        C.oid AS tablerelid,
        I.oid AS indexrelid,
        C.relname AS tablename,
        I.relname AS indexname,
        (vector_stat(I.relfilenode)).*
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid JOIN
         pg_am A ON A.oid = I.relam
    WHERE A.amname = 'vectors';
