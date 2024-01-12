-- finalize begin

-- List of data types

CREATE TYPE vector (
    INPUT = _vectors_vecf32_in,
    OUTPUT = _vectors_vecf32_out,
    TYPMOD_IN = _vectors_typmod_in,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTENDED,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
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

-- List of operators

CREATE OPERATOR + (
	PROCEDURE = _vectors_vecf32_operator_add,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = +
);

CREATE OPERATOR + (
	PROCEDURE = _vectors_vecf16_operator_add,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = +
);

CREATE OPERATOR - (
	PROCEDURE = _vectors_vecf32_operator_minus,
	LEFTARG = vector,
	RIGHTARG = vector
);

CREATE OPERATOR - (
	PROCEDURE = _vectors_vecf16_operator_minus,
	LEFTARG = vecf16,
	RIGHTARG = vecf16
);

CREATE OPERATOR = (
	PROCEDURE = _vectors_vecf32_operator_eq,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = =,
	NEGATOR = <>,
	RESTRICT = eqsel,
	JOIN = eqjoinsel
);

CREATE OPERATOR = (
	PROCEDURE = _vectors_vecf16_operator_eq,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = =,
	NEGATOR = <>,
	RESTRICT = eqsel,
	JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	PROCEDURE = _vectors_vecf32_operator_neq,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = <>,
	NEGATOR = =,
	RESTRICT = eqsel,
	JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	PROCEDURE = _vectors_vecf16_operator_neq,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = <>,
	NEGATOR = =,
	RESTRICT = eqsel,
	JOIN = eqjoinsel
);

CREATE OPERATOR < (
	PROCEDURE = _vectors_vecf32_operator_lt,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = >,
	NEGATOR = >=,
	RESTRICT = scalarltsel,
	JOIN = scalarltjoinsel
);

CREATE OPERATOR < (
	PROCEDURE = _vectors_vecf16_operator_lt,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = >,
	NEGATOR = >=,
	RESTRICT = scalarltsel,
	JOIN = scalarltjoinsel
);

CREATE OPERATOR > (
	PROCEDURE = _vectors_vecf32_operator_gt,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = <,
	NEGATOR = <=,
	RESTRICT = scalargtsel,
	JOIN = scalargtjoinsel
);

CREATE OPERATOR > (
	PROCEDURE = _vectors_vecf16_operator_gt,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = <,
	NEGATOR = <=,
	RESTRICT = scalargtsel,
	JOIN = scalargtjoinsel
);

CREATE OPERATOR <= (
	PROCEDURE = _vectors_vecf32_operator_lte,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = >=,
	NEGATOR = >,
	RESTRICT = scalarltsel,
	JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
	PROCEDURE = _vectors_vecf16_operator_lte,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = >=,
	NEGATOR = >,
	RESTRICT = scalarltsel,
	JOIN = scalarltjoinsel
);

CREATE OPERATOR >= (
	PROCEDURE = _vectors_vecf32_operator_gte,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = <=,
	NEGATOR = <,
	RESTRICT = scalargtsel,
	JOIN = scalargtjoinsel
);

CREATE OPERATOR >= (
	PROCEDURE = _vectors_vecf16_operator_gte,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = <=,
	NEGATOR = <,
	RESTRICT = scalargtsel,
	JOIN = scalargtjoinsel
);

CREATE OPERATOR <-> (
	PROCEDURE = _vectors_vecf32_operator_l2,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = <->
);

CREATE OPERATOR <-> (
	PROCEDURE = _vectors_vecf16_operator_l2,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = <->
);

CREATE OPERATOR <#> (
	PROCEDURE = _vectors_vecf32_operator_dot,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = <#>
);

CREATE OPERATOR <#> (
	PROCEDURE = _vectors_vecf16_operator_dot,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = <#>
);

CREATE OPERATOR <=> (
	PROCEDURE = _vectors_vecf32_operator_cosine,
	LEFTARG = vector,
	RIGHTARG = vector,
	COMMUTATOR = <=>
);

CREATE OPERATOR <=> (
	PROCEDURE = _vectors_vecf16_operator_cosine,
	LEFTARG = vecf16,
	RIGHTARG = vecf16,
	COMMUTATOR = <=>
);

-- List of functions

-- List of casts

CREATE CAST (real[] AS vector)
    WITH FUNCTION _vectors_cast_array_to_vecf32(real[], integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
    WITH FUNCTION _vectors_cast_vecf32_to_array(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS vecf16)
    WITH FUNCTION _vectors_cast_vecf32_to_vecf16(vector, integer, boolean) AS IMPLICIT;

-- List of access methods

CREATE ACCESS METHOD vectors TYPE INDEX HANDLER _vectors_amhandler;
COMMENT ON ACCESS METHOD vectors IS 'pgvecto.rs index access method';

-- List of operator classes

CREATE OPERATOR CLASS vector_l2_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vector_dot_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS vector_cos_ops
    FOR TYPE vector USING vectors AS
    OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops;

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

-- finalize end
