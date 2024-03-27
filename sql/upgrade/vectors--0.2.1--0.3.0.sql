-- List of shell types

CREATE TYPE svector;
CREATE TYPE bvector;
CREATE TYPE veci8;

-- List of internal functions

CREATE FUNCTION _vectors_veci8_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_subscript_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_send"(
    "vector" veci8
) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_send_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_recv"(
    "internal" internal,
    "oid" oid,
    "typmod" INT
) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_recv_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_out"(
    "vector" veci8
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_out_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_neq"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_neq_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_minus"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_minus_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_lte"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_lte_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_lt"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_lt_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_l2"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_l2_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_gte"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_gte_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_gt"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_gt_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_eq"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_eq_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_dot"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_dot_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_cosine"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_cosine_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_operator_add"(
    "lhs" veci8,
    "rhs" veci8
) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_operator_add_wrapper';

CREATE FUNCTION vectors."_vectors_veci8_in"(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_veci8_in_wrapper';

CREATE FUNCTION _vectors_vecf32_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_subscript_wrapper';

CREATE FUNCTION vectors."_vectors_vecf32_send"(
    "vector" vector
) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_send_wrapper';

CREATE FUNCTION vectors."_vectors_vecf32_recv"(
    "internal" internal,
    "oid" oid,
    "typmod" INT
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_recv_wrapper';

CREATE FUNCTION _vectors_vecf16_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_subscript_wrapper';

CREATE FUNCTION vectors."_vectors_vecf16_send"(
    "vector" vecf16
) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_send_wrapper';

CREATE FUNCTION vectors."_vectors_vecf16_recv"(
    "internal" internal,
    "oid" oid,
    "typmod" INT
) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_recv_wrapper';

CREATE FUNCTION vectors."_vectors_typmod_in_65535"(
    "list" cstring[]
) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_typmod_in_65535_wrapper';

CREATE FUNCTION vectors."_vectors_typmod_in_1048575"(
    "list" cstring[]
) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_typmod_in_1048575_wrapper';

CREATE FUNCTION vectors."_vectors_to_veci8"(
    "len" INT,
    "alpha" real,
    "offset" real,
    "values" INT[]
) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_veci8_wrapper';

CREATE FUNCTION vectors."_vectors_to_svector"(
    "dims" INT,
    "index" INT[],
    "value" real[]
) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_svector_wrapper';

CREATE FUNCTION vectors."_vectors_text2vec_openai"(
    "input" TEXT,
    "model" TEXT
) RETURNS vector
STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_text2vec_openai_wrapper';

CREATE FUNCTION _vectors_svecf32_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_subscript_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_send"(
    "vector" svector
) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_send_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_recv"(
    "internal" internal,
    "oid" oid,
    "typmod" INT
) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_recv_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_out"(
    "vector" svector
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_out_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_neq"(
    "lhs" svector,
    "rhs" svector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_neq_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_minus"(
    "lhs" svector,
    "rhs" svector
) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_minus_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_lte"(
    "lhs" svector,
    "rhs" svector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_lte_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_lt"(
    "lhs" svector,
    "rhs" svector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_lt_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_l2"(
    "lhs" svector,
    "rhs" svector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_l2_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_gte"(
    "lhs" svector,
    "rhs" svector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_gte_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_gt"(
    "lhs" svector,
    "rhs" svector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_gt_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_eq"(
    "lhs" svector,
    "rhs" svector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_eq_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_dot"(
    "lhs" svector,
    "rhs" svector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_dot_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_cosine"(
    "lhs" svector,
    "rhs" svector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_cosine_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_operator_add"(
    "lhs" svector,
    "rhs" svector
) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_operator_add_wrapper';

CREATE FUNCTION vectors."_vectors_svecf32_in"(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_in_wrapper';

CREATE OR REPLACE FUNCTION vectors."_vectors_pgvectors_upgrade"() RETURNS void
STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

CREATE OR REPLACE FUNCTION vectors."_vectors_index_stat"(
    "oid" oid
) RETURNS vectors.vector_index_stat /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_index_stat_wrapper';

CREATE FUNCTION vectors."_vectors_cast_veci8_to_vecf32"(
    "vector" veci8,
    "_typmod" INT,
    "_explicit" bool
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_veci8_to_vecf32_wrapper';

CREATE FUNCTION vectors."_vectors_cast_vecf32_to_veci8"(
    "vector" vector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_veci8_wrapper';

CREATE FUNCTION vectors."_vectors_cast_vecf32_to_svecf32"(
    "vector" vector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_svecf32_wrapper';

CREATE FUNCTION vectors."_vectors_cast_vecf32_to_bvecf32"(
    "vector" vector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_vecf32_to_bvecf32_wrapper';

CREATE FUNCTION vectors."_vectors_cast_vecf16_to_vecf32"(
    "vector" vecf16,
    "_typmod" INT,
    "_explicit" bool
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_vecf16_to_vecf32_wrapper';

CREATE FUNCTION vectors."_vectors_cast_svecf32_to_vecf32"(
    "vector" svector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_svecf32_to_vecf32_wrapper';

CREATE FUNCTION vectors."_vectors_cast_bvecf32_to_vecf32"(
    "vector" bvector,
    "_typmod" INT,
    "_explicit" bool
) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_cast_bvecf32_to_vecf32_wrapper';

CREATE FUNCTION _vectors_bvecf32_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_subscript_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_send"(
    "vector" bvector
) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_send_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_recv"(
    "internal" internal,
    "oid" oid,
    "typmod" INT
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_recv_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_out"(
    "vector" bvector
) RETURNS cstring
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_out_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_xor"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_xor_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_or"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_or_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_neq"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_neq_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_lte"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_lte_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_lt"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_lt_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_l2"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_l2_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_jaccard"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_jaccard_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_gte"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_gte_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_gt"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_gt_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_eq"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bool
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_eq_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_dot"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_dot_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_cosine"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_cosine_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_operator_and"(
    "lhs" bvector,
    "rhs" bvector
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_operator_and_wrapper';

CREATE FUNCTION vectors."_vectors_bvecf32_in"(
    "input" cstring,
    "_oid" oid,
    "typmod" INT
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvecf32_in_wrapper';

CREATE FUNCTION vectors."_vectors_binarize"(
    "vector" vector
) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_binarize_wrapper';

-- List of data types

ALTER TYPE vector SET (
    RECEIVE = _vectors_vecf32_recv,
    SEND = _vectors_vecf32_send,
    SUBSCRIPT = _vectors_vecf32_subscript,
    TYPMOD_IN = _vectors_typmod_in_65535,
    STORAGE = EXTERNAL
);

ALTER TYPE vecf16 SET (
    RECEIVE = _vectors_vecf16_recv,
    SEND = _vectors_vecf16_send,
    SUBSCRIPT = _vectors_vecf16_subscript,
    TYPMOD_IN = _vectors_typmod_in_65535,
    STORAGE = EXTERNAL
);

CREATE TYPE svector (
    INPUT = _vectors_svecf32_in,
    OUTPUT = _vectors_svecf32_out,
    RECEIVE = _vectors_svecf32_recv,
    SEND = _vectors_svecf32_send,
    SUBSCRIPT = _vectors_svecf32_subscript,
    TYPMOD_IN = _vectors_typmod_in_1048575,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTERNAL,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);

CREATE TYPE bvector (
    INPUT = _vectors_bvecf32_in,
    OUTPUT = _vectors_bvecf32_out,
    RECEIVE = _vectors_bvecf32_recv,
    SEND = _vectors_bvecf32_send,
    SUBSCRIPT = _vectors_bvecf32_subscript,
    TYPMOD_IN = _vectors_typmod_in_65535,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTERNAL,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);

CREATE TYPE veci8 (
    INPUT = _vectors_veci8_in,
    OUTPUT = _vectors_veci8_out,
    RECEIVE = _vectors_veci8_recv,
    SEND = _vectors_veci8_send,
    SUBSCRIPT = _vectors_veci8_subscript,
    TYPMOD_IN = _vectors_typmod_in_65535,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTERNAL,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);

-- List of operators

CREATE OPERATOR + (
    PROCEDURE = _vectors_svecf32_operator_add,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = +
);

CREATE OPERATOR + (
    PROCEDURE = _vectors_veci8_operator_add,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = +
);

CREATE OPERATOR - (
    PROCEDURE = _vectors_svecf32_operator_minus,
    LEFTARG = svector,
    RIGHTARG = svector
);

CREATE OPERATOR - (
    PROCEDURE = _vectors_veci8_operator_minus,
    LEFTARG = veci8,
    RIGHTARG = veci8
);

CREATE OPERATOR & (
    PROCEDURE = _vectors_bvecf32_operator_and,
    LEFTARG = bvector,
    RIGHTARG = bvector
);

CREATE OPERATOR | (
    PROCEDURE = _vectors_bvecf32_operator_or,
    LEFTARG = bvector,
    RIGHTARG = bvector
);

CREATE OPERATOR ^ (
    PROCEDURE = _vectors_bvecf32_operator_xor,
    LEFTARG = bvector,
    RIGHTARG = bvector
);

CREATE OPERATOR = (
    PROCEDURE = _vectors_svecf32_operator_eq,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR = (
    PROCEDURE = _vectors_bvecf32_operator_eq,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR = (
    PROCEDURE = _vectors_veci8_operator_eq,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR <> (
    PROCEDURE = _vectors_svecf32_operator_neq,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR <> (
    PROCEDURE = _vectors_bvecf32_operator_neq,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR <> (
    PROCEDURE = _vectors_veci8_operator_neq,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR < (
    PROCEDURE = _vectors_svecf32_operator_lt,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR < (
    PROCEDURE = _vectors_bvecf32_operator_lt,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR < (
    PROCEDURE = _vectors_veci8_operator_lt,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR > (
    PROCEDURE = _vectors_svecf32_operator_gt,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR > (
    PROCEDURE = _vectors_bvecf32_operator_gt,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR > (
    PROCEDURE = _vectors_veci8_operator_gt,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR <= (
    PROCEDURE = _vectors_svecf32_operator_lte,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    PROCEDURE = _vectors_bvecf32_operator_lte,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    PROCEDURE = _vectors_veci8_operator_lte,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR >= (
    PROCEDURE = _vectors_svecf32_operator_gte,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR >= (
    PROCEDURE = _vectors_bvecf32_operator_gte,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR >= (
    PROCEDURE = _vectors_veci8_operator_gte,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR <-> (
    PROCEDURE = _vectors_svecf32_operator_l2,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <->
);

CREATE OPERATOR <-> (
    PROCEDURE = _vectors_bvecf32_operator_l2,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <->
);

CREATE OPERATOR <-> (
    PROCEDURE = _vectors_veci8_operator_l2,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = <->
);

CREATE OPERATOR <#> (
    PROCEDURE = _vectors_svecf32_operator_dot,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <#>
);

CREATE OPERATOR <#> (
    PROCEDURE = _vectors_bvecf32_operator_dot,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <#>
);

CREATE OPERATOR <#> (
    PROCEDURE = _vectors_veci8_operator_dot,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = <#>
);

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_svecf32_operator_cosine,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <=>
);

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_bvecf32_operator_cosine,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <=>
);

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_veci8_operator_cosine,
    LEFTARG = veci8,
    RIGHTARG = veci8,
    COMMUTATOR = <=>
);

CREATE OPERATOR <~> (
    PROCEDURE = _vectors_bvecf32_operator_jaccard,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <~>
);

-- List of functions

CREATE OR REPLACE FUNCTION pgvectors_upgrade() RETURNS void
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

CREATE FUNCTION to_svector("dims" INT, "indexes" INT[], "values" real[]) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_svector_wrapper';

CREATE FUNCTION to_veci8("len" INT, "alpha" real, "offset" real, "values" INT[]) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_veci8_wrapper';

CREATE FUNCTION binarize("vector" vector) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_binarize_wrapper';

CREATE FUNCTION text2vec_openai("input" TEXT, "model" TEXT) RETURNS vector
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_text2vec_openai_wrapper';

CREATE FUNCTION text2vec_openai_v3(input TEXT) RETURNS vector
STRICT PARALLEL SAFE LANGUAGE plpgsql AS
$$
DECLARE 
variable vectors.vector;
BEGIN
  variable := vectors.text2vec_openai(input, 'text-embedding-3-small');
  RETURN variable;
END;
$$;

-- List of casts

CREATE CAST (vecf16 AS vector)
    WITH FUNCTION _vectors_cast_vecf16_to_vecf32(vecf16, integer, boolean);

CREATE CAST (vector AS svector)
    WITH FUNCTION _vectors_cast_vecf32_to_svecf32(vector, integer, boolean);

CREATE CAST (svector AS vector)
    WITH FUNCTION _vectors_cast_svecf32_to_vecf32(svector, integer, boolean);

CREATE CAST (vector AS bvector)
    WITH FUNCTION _vectors_cast_vecf32_to_bvecf32(vector, integer, boolean);

CREATE CAST (bvector AS vector)
    WITH FUNCTION _vectors_cast_bvecf32_to_vecf32(bvector, integer, boolean);

CREATE CAST (veci8 AS vector)
    WITH FUNCTION _vectors_cast_veci8_to_vecf32(veci8, integer, boolean);

CREATE CAST (vector AS veci8)
    WITH FUNCTION _vectors_cast_vecf32_to_veci8(vector, integer, boolean);

-- List of operator families

CREATE OPERATOR FAMILY svector_l2_ops USING vectors;

CREATE OPERATOR FAMILY svector_dot_ops USING vectors;

CREATE OPERATOR FAMILY svector_cos_ops USING vectors;

CREATE OPERATOR FAMILY bvector_l2_ops USING vectors;

CREATE OPERATOR FAMILY bvector_dot_ops USING vectors;

CREATE OPERATOR FAMILY bvector_cos_ops USING vectors;

CREATE OPERATOR FAMILY bvector_jaccard_ops USING vectors;

CREATE OPERATOR FAMILY veci8_l2_ops USING vectors;

CREATE OPERATOR FAMILY veci8_dot_ops USING vectors;

CREATE OPERATOR FAMILY veci8_cos_ops USING vectors;

-- List of operator classes

CREATE OPERATOR CLASS svector_l2_ops
    FOR TYPE svector USING vectors FAMILY svector_l2_ops AS
    OPERATOR 1 <-> (svector, svector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS svector_dot_ops
    FOR TYPE svector USING vectors FAMILY svector_dot_ops AS
    OPERATOR 1 <#> (svector, svector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS svector_cos_ops
    FOR TYPE svector USING vectors FAMILY svector_cos_ops AS
    OPERATOR 1 <=> (svector, svector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_l2_ops
    FOR TYPE bvector USING vectors FAMILY bvector_l2_ops AS
    OPERATOR 1 <-> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_dot_ops
    FOR TYPE bvector USING vectors FAMILY bvector_dot_ops AS
    OPERATOR 1 <#> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_cos_ops
    FOR TYPE bvector USING vectors FAMILY bvector_cos_ops AS
    OPERATOR 1 <=> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_jaccard_ops
    FOR TYPE bvector USING vectors FAMILY bvector_jaccard_ops AS
    OPERATOR 1 <~> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS veci8_l2_ops
    FOR TYPE veci8 USING vectors FAMILY veci8_l2_ops AS
    OPERATOR 1 <-> (veci8, veci8) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS veci8_dot_ops
    FOR TYPE veci8 USING vectors FAMILY veci8_dot_ops AS
    OPERATOR 1 <#> (veci8, veci8) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS veci8_cos_ops
    FOR TYPE veci8 USING vectors FAMILY veci8_cos_ops AS
    OPERATOR 1 <=> (veci8, veci8) FOR ORDER BY float_ops;

-- List of views

CREATE OR REPLACE VIEW pg_vector_index_stat AS
    SELECT
        C.oid AS tablerelid,
        I.oid AS indexrelid,
        C.relname AS tablename,
        I.relname AS indexname,
        (_vectors_index_stat(I.oid)).*
    FROM pg_class C JOIN
         pg_index X ON C.oid = X.indrelid JOIN
         pg_class I ON I.oid = X.indexrelid JOIN
         pg_am A ON A.oid = I.relam
    WHERE A.amname = 'vectors';

GRANT SELECT ON TABLE pg_vector_index_stat TO PUBLIC;

-- Cleanup

DROP FUNCTION _vectors_ai_embedding_vector;
DROP FUNCTION _vectors_typmod_in;
