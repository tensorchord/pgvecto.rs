\echo Use "ALTER EXTENSION vectors UPDATE TO '0.2.0'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        IF '@extschema@' != 'vectors' THEN
            RAISE EXCEPTION 'Please read upgrade document in https://docs.pgvecto.rs/admin/upgrading.html#upgrade-from-0-1-x.';
        END IF;
    END;
$$;

CREATE TYPE VectorIndexInfo AS (
    indexing BOOL,
    idx_tuples INT,
    idx_sealed_len INT,
    idx_growing_len INT,
    idx_write INT,
    idx_sealed INT[],
    idx_growing INT[],
    idx_config TEXT
);

CREATE FUNCTION "vector_stat"(
    "oid" oid
) RETURNS VectorIndexInfo
STRICT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', 'vector_stat_wrapper';

CREATE VIEW pg_vector_index_info AS
    SELECT
        C.oid AS tablerelid,
        I.oid AS indexrelid,
        C.relname AS tablename,
        I.relname AS indexname,
        (vector_stat(I.oid)).*
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid JOIN
         pg_am A ON A.oid = I.relam
    WHERE A.amname = 'vectors';
