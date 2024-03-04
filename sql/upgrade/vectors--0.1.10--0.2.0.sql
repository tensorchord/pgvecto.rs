\echo Use "ALTER EXTENSION vectors UPDATE TO '0.2.0'" to load this file. \quit

DO LANGUAGE plpgsql $$
    DECLARE
    BEGIN
        IF '@extschema@' != 'vectors' THEN
            RAISE EXCEPTION 'Please read upgrade document in https://docs.pgvecto.rs/admin/upgrading.html#upgrade-from-0-1-x.';
        END IF;
    END;
$$;

CREATE TYPE vector_index_stat AS (
    idx_status TEXT,
    idx_indexing BOOL,
    idx_tuples BIGINT,
    idx_sealed BIGINT[],
    idx_growing BIGINT[],
    idx_write BIGINT,
    idx_size BIGINT,
    idx_options TEXT
);

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

-- List of shell types

CREATE TYPE vecf16;

-- Add new internal functions

CREATE FUNCTION "_vectors_vecf16_out"(
    "vector" vecf16
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_out_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_neq"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_neq_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_minus"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_minus_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_lte"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_lte_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_lt"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_lt_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_l2"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_l2_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_gte"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_gte_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_gt"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_gt_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_eq"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_eq_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_dot"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_dot_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_cosine"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_cosine_wrapper';

CREATE FUNCTION "_vectors_vecf16_operator_add"(
    "lhs" vecf16,
    "rhs" vecf16
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_operator_add_wrapper';

CREATE FUNCTION "_vectors_vecf16_in"(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_in_wrapper';

CREATE FUNCTION "_vectors_typmod_out"(
    "typmod" INT
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_typmod_out_wrapper';

CREATE FUNCTION "_vectors_typmod_in"(
    "list" cstring[]
) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_typmod_in_wrapper';

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

CREATE FUNCTION "_vectors_pgvectors_upgrade"() RETURNS void
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

CREATE FUNCTION "_vectors_ai_embedding_vector"(
    "input" TEXT
) RETURNS vector
STRICT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_ai_embedding_vector_wrapper';

-- List of data types

ALTER FUNCTION vector_in(cstring, oid, INT) RENAME TO _vectors_vecf32_in;
CREATE OR REPLACE FUNCTION _vectors_vecf32_in(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_in_wrapper';
ALTER FUNCTION vector_out(vector) RENAME TO _vectors_vecf32_out;
CREATE OR REPLACE FUNCTION _vectors_vecf32_out(
    "vector" vector
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_out_wrapper';
ALTER TYPE vector SET (
    TYPMOD_IN = _vectors_typmod_in,
    TYPMOD_OUT = _vectors_typmod_out
);

CREATE TYPE vecf16 (
    INPUT = _vectors_vecf16_in,
    OUTPUT = _vectors_vecf16_out,
    TYPMOD_IN = _vectors_typmod_in,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTENDED,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);

-- List of operators

ALTER FUNCTION operator_add(vector, vector) RENAME TO _vectors_vecf32_operator_add;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_add(
    "lhs" vector,
    "rhs" vector
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_add_wrapper';

CREATE OPERATOR + (
    PROCEDURE = _vectors_vecf16_operator_add,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = +
);

ALTER FUNCTION operator_minus(vector, vector) RENAME TO  _vectors_vecf32_operator_minus;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_minus(
    "lhs" vector,
    "rhs" vector
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_minus_wrapper';

CREATE OPERATOR - (
    PROCEDURE = _vectors_vecf16_operator_minus,
    LEFTARG = vecf16,
    RIGHTARG = vecf16
);

ALTER FUNCTION operator_eq(vector, vector) RENAME TO  _vectors_vecf32_operator_eq;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_eq(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_eq_wrapper';

CREATE OPERATOR = (
    PROCEDURE = _vectors_vecf16_operator_eq,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

ALTER FUNCTION operator_neq(vector, vector) RENAME TO  _vectors_vecf32_operator_neq;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_neq(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_neq_wrapper';

CREATE OPERATOR <> (
    PROCEDURE = _vectors_vecf16_operator_neq,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

ALTER FUNCTION operator_lt(vector, vector) RENAME TO  _vectors_vecf32_operator_lt;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_lt(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_lt_wrapper';

CREATE OPERATOR < (
    PROCEDURE = _vectors_vecf16_operator_lt,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

ALTER FUNCTION operator_gt(vector, vector) RENAME TO  _vectors_vecf32_operator_gt;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_gt(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_gt_wrapper';

CREATE OPERATOR > (
    PROCEDURE = _vectors_vecf16_operator_gt,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

ALTER FUNCTION operator_lte(vector, vector) RENAME TO  _vectors_vecf32_operator_lte;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_lte(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_lte_wrapper';

CREATE OPERATOR <= (
    PROCEDURE = _vectors_vecf16_operator_lte,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

ALTER FUNCTION operator_gte(vector, vector) RENAME TO  _vectors_vecf32_operator_gte;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_gte(
    "lhs" vector,
    "rhs" vector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_gte_wrapper';

CREATE OPERATOR >= (
    PROCEDURE = _vectors_vecf16_operator_gte,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

ALTER FUNCTION operator_l2(vector, vector) RENAME TO  _vectors_vecf32_operator_l2;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_l2(
    "lhs" vector,
    "rhs" vector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_l2_wrapper';

CREATE OPERATOR <-> (
    PROCEDURE = _vectors_vecf16_operator_l2,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <->
);

ALTER FUNCTION operator_dot(vector, vector) RENAME TO  _vectors_vecf32_operator_dot;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_dot(
    "lhs" vector,
    "rhs" vector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_dot_wrapper';

CREATE OPERATOR <#> (
    PROCEDURE = _vectors_vecf16_operator_dot,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <#>
);

ALTER FUNCTION operator_cosine(vector, vector) RENAME TO  _vectors_vecf32_operator_cosine;
CREATE OR REPLACE FUNCTION _vectors_vecf32_operator_cosine(
    "lhs" vector,
    "rhs" vector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_operator_cosine_wrapper';

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_vecf16_operator_cosine,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <=>
);

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

ALTER OPERATOR FAMILY l2_ops USING vectors RENAME TO vector_l2_ops;
ALTER OPERATOR CLASS l2_ops USING vectors RENAME TO vector_l2_ops;

ALTER OPERATOR FAMILY dot_ops USING vectors RENAME TO vector_dot_ops;
ALTER OPERATOR CLASS dot_ops USING vectors RENAME TO vector_dot_ops;

ALTER OPERATOR FAMILY cosine_ops USING vectors RENAME TO vector_cos_ops;
ALTER OPERATOR CLASS cosine_ops USING vectors RENAME TO vector_cos_ops;

CREATE OPERATOR CLASS vecf16_l2_ops
    FOR TYPE vecf16 USING vectors AS
    OPERATOR 1 <-> (vecf16, vecf16) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vecf16_dot_ops
    FOR TYPE vecf16 USING vectors AS
    OPERATOR 1 <#> (vecf16, vecf16) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vecf16_cos_ops
    FOR TYPE vecf16 USING vectors AS
    OPERATOR 1 <=> (vecf16, vecf16) FOR ORDER BY float_ops;

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

DROP FUNCTION vector_typmod_out;

DROP FUNCTION vector_typmod_in;

DROP FUNCTION cast_vector_to_array;

DROP FUNCTION cast_array_to_vector;

DROP FUNCTION ai_embedding_vector;
