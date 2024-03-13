-- finalize begin

-- List of data types

CREATE TYPE vector (
    INPUT = _vectors_vecf32_in,
    OUTPUT = _vectors_vecf32_out,
    RECEIVE = _vectors_vecf32_recv,
    SEND = _vectors_vecf32_send,
    SUBSCRIPT = _vectors_vecf32_subscript,
    TYPMOD_IN = _vectors_typmod_in_65535,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTERNAL,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);

CREATE TYPE vecf16 (
    INPUT = _vectors_vecf16_in,
    OUTPUT = _vectors_vecf16_out,
    RECEIVE = _vectors_vecf16_recv,
    SEND = _vectors_vecf16_send,
    SUBSCRIPT = _vectors_vecf16_subscript,
    TYPMOD_IN = _vectors_typmod_in_65535,
    TYPMOD_OUT = _vectors_typmod_out,
    STORAGE = EXTERNAL,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
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
    PROCEDURE = _vectors_vecf32_operator_minus,
    LEFTARG = vector,
    RIGHTARG = vector
);

CREATE OPERATOR - (
    PROCEDURE = _vectors_vecf16_operator_minus,
    LEFTARG = vecf16,
    RIGHTARG = vecf16
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

CREATE FUNCTION pgvectors_upgrade() RETURNS void
STRICT LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

CREATE FUNCTION to_svector(dims INT, indexes INT[], "values" real[]) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_svector_wrapper';

CREATE FUNCTION to_veci8("len" INT, "alpha" real, "offset" real, "values" INT[]) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_veci8_wrapper';

CREATE FUNCTION binarize("vector" vector) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_binarize_wrapper';

CREATE FUNCTION text2vec_openai(input TEXT, model TEXT) RETURNS vector
STRICT LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_text2vec_openai_wrapper';

CREATE FUNCTION text2vec_openai_v3(input TEXT) RETURNS vector
STRICT LANGUAGE plpgsql AS
$$
DECLARE 
variable vectors.vector;
BEGIN
  variable := vectors.text2vec_openai(input, 'text-embedding-3-small');
  RETURN variable;
END;
$$;

-- List of casts

CREATE CAST (real[] AS vector)
    WITH FUNCTION _vectors_cast_array_to_vecf32(real[], integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
    WITH FUNCTION _vectors_cast_vecf32_to_array(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS vecf16)
    WITH FUNCTION _vectors_cast_vecf32_to_vecf16(vector, integer, boolean);

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

-- List of access methods

CREATE ACCESS METHOD vectors TYPE INDEX HANDLER _vectors_amhandler;
COMMENT ON ACCESS METHOD vectors IS 'pgvecto.rs index access method';

-- List of operator families

CREATE OPERATOR FAMILY vector_l2_ops USING vectors;

CREATE OPERATOR FAMILY vector_dot_ops USING vectors;

CREATE OPERATOR FAMILY vector_cos_ops USING vectors;

CREATE OPERATOR FAMILY vecf16_l2_ops USING vectors;

CREATE OPERATOR FAMILY vecf16_dot_ops USING vectors;

CREATE OPERATOR FAMILY vecf16_cos_ops USING vectors;

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

CREATE OPERATOR CLASS svector_l2_ops
    FOR TYPE svector USING vectors AS
    OPERATOR 1 <-> (svector, svector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS svector_dot_ops
    FOR TYPE svector USING vectors AS
    OPERATOR 1 <#> (svector, svector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS svector_cos_ops
    FOR TYPE svector USING vectors AS
    OPERATOR 1 <=> (svector, svector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_l2_ops
    FOR TYPE bvector USING vectors AS
    OPERATOR 1 <-> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_dot_ops
    FOR TYPE bvector USING vectors AS
    OPERATOR 1 <#> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_cos_ops
    FOR TYPE bvector USING vectors AS
    OPERATOR 1 <=> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS bvector_jaccard_ops
    FOR TYPE bvector USING vectors AS
    OPERATOR 1 <~> (bvector, bvector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS veci8_l2_ops
    FOR TYPE veci8 USING vectors AS
    OPERATOR 1 <-> (veci8, veci8) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS veci8_dot_ops
    FOR TYPE veci8 USING vectors AS
    OPERATOR 1 <#> (veci8, veci8) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS veci8_cos_ops
    FOR TYPE veci8 USING vectors AS
    OPERATOR 1 <=> (veci8, veci8) FOR ORDER BY float_ops;

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

GRANT SELECT ON TABLE pg_vector_index_stat TO PUBLIC;

-- finalize end
