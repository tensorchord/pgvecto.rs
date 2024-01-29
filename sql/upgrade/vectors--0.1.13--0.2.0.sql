\echo Use "ALTER EXTENSION vectors UPDATE TO '0.2.0'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        IF '@extschema@' != 'vectors' THEN
            RAISE EXCEPTION 'Please read upgrade document in https://docs.pgvecto.rs/admin/upgrading.html#upgrade-from-0-1-x.';
        END IF;
    END;
$$;

-- List of shell types

ALTER TYPE VectorIndexStat RENAME TO vector_index_stat;

-- Add new internal functions

CREATE FUNCTION "_vectors_index_stat"(
    "oid" oid
) RETURNS vector_index_stat
STRICT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_index_stat_wrapper';

CREATE FUNCTION "_vectors_cast_vecf32_to_vecf16"(
    "vector" vector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_vecf16_wrapper';

CREATE FUNCTION "_vectors_cast_vecf32_to_array"(
    "vector" vector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS real[]
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_array_wrapper';

CREATE FUNCTION "_vectors_cast_array_to_vecf32"(
    "array" real[],
    "typmod" INT,
    "_explicit" bool
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_array_to_vecf32_wrapper';

CREATE FUNCTION "_vectors_ai_embedding_vector"(
    "input" TEXT
) RETURNS vector
STRICT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_ai_embedding_vector_wrapper';

CREATE FUNCTION "_vectors_pgvectors_upgrade"() RETURNS void
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

-- List of data types

ALTER FUNCTION typmod_in(cstring[]) RENAME TO _vectors_typmod_in;
CREATE OR REPLACE FUNCTION _vectors_typmod_in(
    "list" cstring[]
) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_typmod_in_wrapper';

ALTER FUNCTION typmod_out(INT) RENAME TO _vectors_typmod_out;
CREATE OR REPLACE FUNCTION _vectors_typmod_out(
    "typmod" INT
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_typmod_out_wrapper';

ALTER FUNCTION vecf32_in(cstring, oid, INT) RENAME TO _vectors_vecf32_in;
CREATE OR REPLACE FUNCTION _vectors_vecf32_in(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_in_wrapper';
ALTER FUNCTION vecf32_out(vector) RENAME TO _vectors_vecf32_out;
CREATE OR REPLACE FUNCTION _vectors_vecf32_out(
    "vector" vector
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_out_wrapper';
ALTER TYPE vector SET (
    TYPMOD_IN = _vectors_typmod_in,
    TYPMOD_OUT = _vectors_typmod_out
);

ALTER FUNCTION vecf16_in(cstring, oid, INT) RENAME TO _vectors_vecf16_in;
CREATE OR REPLACE FUNCTION _vectors_vecf16_in(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_in_wrapper';
ALTER FUNCTION vecf16_out(vecf16) RENAME TO _vectors_vecf16_out;
CREATE OR REPLACE FUNCTION _vectors_vecf16_out(
    "vector" vecf16
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_out_wrapper';
ALTER TYPE vecf16 SET (
    TYPMOD_IN = _vectors_typmod_in,
    TYPMOD_OUT = _vectors_typmod_out
);

DROP VIEW pg_vector_index_info;
ALTER TYPE vector_index_stat DROP ATTRIBUTE idx_indexing;
ALTER TYPE vector_index_stat DROP ATTRIBUTE idx_tuples;
ALTER TYPE vector_index_stat DROP ATTRIBUTE idx_sealed;
ALTER TYPE vector_index_stat DROP ATTRIBUTE idx_growing;
ALTER TYPE vector_index_stat DROP ATTRIBUTE idx_write;
ALTER TYPE vector_index_stat DROP ATTRIBUTE idx_options;
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_status TEXT;
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_indexing BOOL;
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_tuples BIGINT;
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_sealed BIGINT[];
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_growing BIGINT[];
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_write BIGINT;
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_size BIGINT;
ALTER TYPE vector_index_stat ADD ATTRIBUTE idx_options TEXT;

-- List of operators

ALTER FUNCTION vecf32_operator_add(vector, vector) RENAME TO _vectors_vecf32_operator_add;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_add(
    "lhs" vector,
    "rhs" vector
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_add_wrapper';

ALTER FUNCTION vecf16_operator_add(vecf16, vecf16) RENAME TO _vectors_vecf16_operator_add;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_add(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_add_wrapper';

ALTER FUNCTION vecf32_operator_minus(vector, vector) RENAME TO  _vectors_vecf32_operator_minus;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_minus(
    "lhs" vector,
    "rhs" vector
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_minus_wrapper';

ALTER FUNCTION vecf16_operator_minus(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_minus;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_minus(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_minus_wrapper';

ALTER FUNCTION vecf32_operator_eq(vector, vector) RENAME TO  _vectors_vecf32_operator_eq;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_eq(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_eq_wrapper';

ALTER FUNCTION vecf16_operator_eq(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_eq;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_eq(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_eq_wrapper';

ALTER FUNCTION vecf32_operator_neq(vector, vector) RENAME TO  _vectors_vecf32_operator_neq;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_neq(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_neq_wrapper';

ALTER FUNCTION vecf16_operator_neq(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_neq;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_neq(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_neq_wrapper';

ALTER FUNCTION vecf32_operator_lt(vector, vector) RENAME TO  _vectors_vecf32_operator_lt;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_lt(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_lt_wrapper';

ALTER FUNCTION vecf16_operator_lt(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_lt;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_lt(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_lt_wrapper';

ALTER FUNCTION vecf32_operator_gt(vector, vector) RENAME TO  _vectors_vecf32_operator_gt;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_gt(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_gt_wrapper';

ALTER FUNCTION vecf16_operator_gt(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_gt;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_gt(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_gt_wrapper';

ALTER FUNCTION vecf32_operator_lte(vector, vector) RENAME TO  _vectors_vecf32_operator_lte;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_lte(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_lte_wrapper';

ALTER FUNCTION vecf16_operator_lte(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_lte;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_lte(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_lte_wrapper';

ALTER FUNCTION vecf32_operator_gte(vector, vector) RENAME TO  _vectors_vecf32_operator_gte;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_gte(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_gte_wrapper';

ALTER FUNCTION vecf16_operator_gte(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_gte;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_gte(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_gte_wrapper';

ALTER FUNCTION vecf32_operator_l2(vector, vector) RENAME TO  _vectors_vecf32_operator_l2;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_l2(
    "lhs" vector,
    "rhs" vector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_l2_wrapper';

ALTER FUNCTION vecf16_operator_l2(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_l2;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_l2(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_l2_wrapper';

ALTER FUNCTION vecf32_operator_dot(vector, vector) RENAME TO  _vectors_vecf32_operator_dot;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_dot(
    "lhs" vector,
    "rhs" vector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_dot_wrapper';

ALTER FUNCTION vecf16_operator_dot(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_dot;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_dot(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_dot_wrapper';

ALTER FUNCTION vecf32_operator_cosine(vector, vector) RENAME TO  _vectors_vecf32_operator_cosine;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_cosine(
    "lhs" vector,
    "rhs" vector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_cosine_wrapper';

ALTER FUNCTION vecf16_operator_cosine(vecf16, vecf16) RENAME TO  _vectors_vecf16_operator_cosine;
CREATE OR REPLACE FUNCTION _vectors_vecf16_operator_cosine(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_cosine_wrapper';

-- List of functions

CREATE FUNCTION pgvectors_upgrade() RETURNS void
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

-- List of casts

DROP CAST (real[] AS vector);
CREATE CAST (real[] AS vector)
    WITH FUNCTION _vectors_cast_array_to_vecf32(real[], integer, boolean) AS IMPLICIT;

DROP CAST (vector AS real[]);
CREATE CAST (vector AS real[])
    WITH FUNCTION _vectors_cast_vecf32_to_array(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS vecf16)
    WITH FUNCTION _vectors_cast_vecf32_to_vecf16(vector, integer, boolean);

-- List of access methods

ALTER FUNCTION vectors_amhandler(internal) RENAME TO _vectors_amhandler;
CREATE OR REPLACE FUNCTION _vectors_amhandler(internal) RETURNS index_am_handler
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_amhandler_wrapper';

-- List of operator classes

-- List of views

CREATE VIEW pg_vector_index_stat AS
    SELECT
        C.oid AS tablerelid,
        I.oid AS indexrelid,
        C.relname AS tablename,
        I.relname AS indexname,
        (_vectors_index_stat(I.relfilenode)).*
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid JOIN
         pg_am A ON A.oid = I.relam
    WHERE A.amname = 'vectors';

-- Drop previous internal functions

DROP FUNCTION vector_stat;

DROP FUNCTION vecf32_cast_array_to_vector;

DROP FUNCTION vecf32_cast_vector_to_array;

DROP FUNCTION ai_embedding_vector;
