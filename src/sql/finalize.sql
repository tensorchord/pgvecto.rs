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
    INPUT = _vectors_bvector_in,
    OUTPUT = _vectors_bvector_out,
    RECEIVE = _vectors_bvector_recv,
    SEND = _vectors_bvector_send,
    SUBSCRIPT = _vectors_bvector_subscript,
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

CREATE TYPE sphere_vector AS (
    center vector,
    radius REAL
);

CREATE TYPE sphere_vecf16 AS (
    center vecf16,
    radius REAL
);

CREATE TYPE sphere_svector AS (
    center svector,
    radius REAL
);

CREATE TYPE sphere_bvector AS (
    center bvector,
    radius REAL
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

CREATE OPERATOR - (
    PROCEDURE = _vectors_vecf32_operator_sub,
    LEFTARG = vector,
    RIGHTARG = vector
);

CREATE OPERATOR - (
    PROCEDURE = _vectors_vecf16_operator_sub,
    LEFTARG = vecf16,
    RIGHTARG = vecf16
);

CREATE OPERATOR - (
    PROCEDURE = _vectors_svecf32_operator_sub,
    LEFTARG = svector,
    RIGHTARG = svector
);

CREATE OPERATOR * (
    PROCEDURE = _vectors_vecf32_operator_mul,
    LEFTARG = vector,
    RIGHTARG = vector,
    COMMUTATOR = *
);

CREATE OPERATOR * (
    PROCEDURE = _vectors_vecf16_operator_mul,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = *
);

CREATE OPERATOR * (
    PROCEDURE = _vectors_svecf32_operator_mul,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = *
);

CREATE OPERATOR & (
    PROCEDURE = _vectors_bvector_operator_and,
    LEFTARG = bvector,
    RIGHTARG = bvector
);

CREATE OPERATOR | (
    PROCEDURE = _vectors_bvector_operator_or,
    LEFTARG = bvector,
    RIGHTARG = bvector
);

CREATE OPERATOR ^ (
    PROCEDURE = _vectors_bvector_operator_xor,
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
    PROCEDURE = _vectors_bvector_operator_eq,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_neq,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_lt,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_gt,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_lte,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_gte,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_hamming,
    LEFTARG = bvector,
    RIGHTARG = bvector,
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
    PROCEDURE = _vectors_bvector_operator_dot,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <#>
);

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_vecf32_operator_cos,
    LEFTARG = vector,
    RIGHTARG = vector,
    COMMUTATOR = <=>
);

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_vecf16_operator_cos,
    LEFTARG = vecf16,
    RIGHTARG = vecf16,
    COMMUTATOR = <=>
);

CREATE OPERATOR <=> (
    PROCEDURE = _vectors_svecf32_operator_cos,
    LEFTARG = svector,
    RIGHTARG = svector,
    COMMUTATOR = <=>
);

CREATE OPERATOR <~> (
    PROCEDURE = _vectors_bvector_operator_jaccard,
    LEFTARG = bvector,
    RIGHTARG = bvector,
    COMMUTATOR = <~>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_vecf32_sphere_l2_in,
    LEFTARG = vector,
    RIGHTARG = sphere_vector,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_vecf16_sphere_l2_in,
    LEFTARG = vecf16,
    RIGHTARG = sphere_vecf16,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_svecf32_sphere_l2_in,
    LEFTARG = svector,
    RIGHTARG = sphere_svector,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<->> (
    PROCEDURE = _vectors_bvector_sphere_hamming_in,
    LEFTARG = bvector,
    RIGHTARG = sphere_bvector,
    COMMUTATOR = <<->>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_vecf32_sphere_dot_in,
    LEFTARG = vector,
    RIGHTARG = sphere_vector,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_vecf16_sphere_dot_in,
    LEFTARG = vecf16,
    RIGHTARG = sphere_vecf16,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_svecf32_sphere_dot_in,
    LEFTARG = svector,
    RIGHTARG = sphere_svector,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<#>> (
    PROCEDURE = _vectors_bvector_sphere_dot_in,
    LEFTARG = bvector,
    RIGHTARG = sphere_bvector,
    COMMUTATOR = <<#>>
);

CREATE OPERATOR <<=>> (
    PROCEDURE = _vectors_vecf32_sphere_cos_in,
    LEFTARG = vector,
    RIGHTARG = sphere_vector,
    COMMUTATOR = <<=>>
);

CREATE OPERATOR <<=>> (
    PROCEDURE = _vectors_vecf16_sphere_cos_in,
    LEFTARG = vecf16,
    RIGHTARG = sphere_vecf16,
    COMMUTATOR = <<=>>
);

CREATE OPERATOR <<=>> (
    PROCEDURE = _vectors_svecf32_sphere_cos_in,
    LEFTARG = svector,
    RIGHTARG = sphere_svector,
    COMMUTATOR = <<=>>
);

CREATE OPERATOR <<~>> (
    PROCEDURE = _vectors_bvector_sphere_jaccard_in,
    LEFTARG = bvector,
    RIGHTARG = sphere_bvector,
    COMMUTATOR = <<~>>
);

-- List of functions

CREATE FUNCTION pgvectors_upgrade() RETURNS void
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_pgvectors_upgrade_wrapper';

CREATE FUNCTION text2vec_openai("input" TEXT, "model" TEXT) RETURNS vector
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_text2vec_openai_wrapper';

CREATE FUNCTION text2vec_openai_v3(input TEXT) RETURNS vector
STRICT PARALLEL SAFE LANGUAGE plpgsql AS
$$
DECLARE 
variable vector;
BEGIN
  variable := text2vec_openai(input, 'text-embedding-3-small');
  RETURN variable;
END;
$$;

CREATE FUNCTION alter_vector_index("index" OID, "key" TEXT, "value" TEXT) RETURNS void
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_alter_vector_index_wrapper';

CREATE FUNCTION fence_vector_index(oid) RETURNS void
STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_fence_vector_index_wrapper';

CREATE FUNCTION vector_dims(vector) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_dims_wrapper';

CREATE FUNCTION vector_dims(vecf16) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_dims_wrapper';

CREATE FUNCTION vector_dims(svector) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_dims_wrapper';

CREATE FUNCTION vector_dims(bvector) RETURNS INT
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvector_dims_wrapper';

CREATE FUNCTION vector_norm(vector) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_norm_wrapper';

CREATE FUNCTION vector_norm(vecf16) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_norm_wrapper';

CREATE FUNCTION vector_norm(svector) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_norm_wrapper';

CREATE FUNCTION vector_norm(bvector) RETURNS real
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_bvector_norm_wrapper';

CREATE FUNCTION vector_normalize(vector) RETURNS vector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf32_normalize_wrapper';

CREATE FUNCTION vector_normalize(vecf16) RETURNS vecf16
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_vecf16_normalize_wrapper';

CREATE FUNCTION vector_normalize(svector) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_svecf32_normalize_wrapper';

CREATE FUNCTION to_svector("dims" INT, "indexes" INT[], "values" real[]) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_to_svector_wrapper';

CREATE FUNCTION binarize("vector" vector) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '_vectors_binarize_wrapper';

CREATE FUNCTION sphere(vector, real) RETURNS sphere_vector
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

CREATE FUNCTION sphere(vecf16, real) RETURNS sphere_vecf16
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

CREATE FUNCTION sphere(svector, real) RETURNS sphere_svector
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

CREATE FUNCTION sphere(bvector, real) RETURNS sphere_bvector
IMMUTABLE PARALLEL SAFE LANGUAGE sql AS 'SELECT ROW($1, $2)';

-- List of aggregates

CREATE AGGREGATE avg(vector) (
    SFUNC = _vectors_vecf32_aggregate_avg_sum_sfunc,
    STYPE = internal,
    COMBINEFUNC = _vectors_vecf32_aggregate_avg_sum_combinefunc,
    FINALFUNC = _vectors_vecf32_aggregate_avg_finalfunc,
    PARALLEL = SAFE
);

CREATE AGGREGATE sum(vector) (
    SFUNC = _vectors_vecf32_aggregate_avg_sum_sfunc,
    STYPE = internal,
    COMBINEFUNC = _vectors_vecf32_aggregate_avg_sum_combinefunc,
    FINALFUNC = _vectors_vecf32_aggregate_sum_finalfunc,
    PARALLEL = SAFE
);

CREATE AGGREGATE avg(svector) (
    SFUNC = _vectors_svecf32_aggregate_avg_sum_sfunc,
    STYPE = internal,
    COMBINEFUNC = _vectors_svecf32_aggregate_avg_sum_combinefunc,
    FINALFUNC = _vectors_svecf32_aggregate_avg_finalfunc,
    PARALLEL = SAFE
);

CREATE AGGREGATE sum(svector) (
    SFUNC = _vectors_svecf32_aggregate_avg_sum_sfunc,
    STYPE = internal,
    COMBINEFUNC = _vectors_svecf32_aggregate_avg_sum_combinefunc,
    FINALFUNC = _vectors_svecf32_aggregate_sum_finalfunc,
    PARALLEL = SAFE
);

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
    WITH FUNCTION _vectors_cast_vecf32_to_bvector(vector, integer, boolean);

CREATE CAST (bvector AS vector)
    WITH FUNCTION _vectors_cast_bvector_to_vecf32(bvector, integer, boolean);

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

CREATE OPERATOR FAMILY bvector_hamming_ops USING vectors;

CREATE OPERATOR FAMILY bvector_dot_ops USING vectors;

CREATE OPERATOR FAMILY bvector_jaccard_ops USING vectors;

-- List of operator classes

CREATE OPERATOR CLASS vector_l2_ops
    FOR TYPE vector USING vectors FAMILY vector_l2_ops AS
    OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
    OPERATOR 2 <<->> (vector, sphere_vector) FOR SEARCH;

CREATE OPERATOR CLASS vector_dot_ops
    FOR TYPE vector USING vectors FAMILY vector_dot_ops AS
    OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (vector, sphere_vector) FOR SEARCH;

CREATE OPERATOR CLASS vector_cos_ops
    FOR TYPE vector USING vectors FAMILY vector_cos_ops AS
    OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (vector, sphere_vector) FOR SEARCH;

CREATE OPERATOR CLASS vecf16_l2_ops
    FOR TYPE vecf16 USING vectors FAMILY vecf16_l2_ops AS
    OPERATOR 1 <-> (vecf16, vecf16) FOR ORDER BY float_ops,
    OPERATOR 2 <<->> (vecf16, sphere_vecf16) FOR SEARCH;

CREATE OPERATOR CLASS vecf16_dot_ops
    FOR TYPE vecf16 USING vectors FAMILY vecf16_dot_ops AS
    OPERATOR 1 <#> (vecf16, vecf16) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (vecf16, sphere_vecf16) FOR SEARCH;

CREATE OPERATOR CLASS vecf16_cos_ops
    FOR TYPE vecf16 USING vectors FAMILY vecf16_cos_ops AS
    OPERATOR 1 <=> (vecf16, vecf16) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (vecf16, sphere_vecf16) FOR SEARCH;

CREATE OPERATOR CLASS svector_l2_ops
    FOR TYPE svector USING vectors FAMILY svector_l2_ops AS
    OPERATOR 1 <-> (svector, svector) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (svector, sphere_svector) FOR SEARCH;

CREATE OPERATOR CLASS svector_dot_ops
    FOR TYPE svector USING vectors FAMILY svector_dot_ops AS
    OPERATOR 1 <#> (svector, svector) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (svector, sphere_svector) FOR SEARCH;

CREATE OPERATOR CLASS svector_cos_ops
    FOR TYPE svector USING vectors FAMILY svector_cos_ops AS
    OPERATOR 1 <=> (svector, svector) FOR ORDER BY float_ops,
    OPERATOR 2 <<=>> (svector, sphere_svector) FOR SEARCH;

CREATE OPERATOR CLASS bvector_hamming_ops
    FOR TYPE bvector USING vectors FAMILY bvector_hamming_ops AS
    OPERATOR 1 <-> (bvector, bvector) FOR ORDER BY float_ops,
    OPERATOR 2 <<->> (bvector, sphere_bvector) FOR SEARCH;

CREATE OPERATOR CLASS bvector_dot_ops
    FOR TYPE bvector USING vectors FAMILY bvector_dot_ops AS
    OPERATOR 1 <#> (bvector, bvector) FOR ORDER BY float_ops,
    OPERATOR 2 <<#>> (bvector, sphere_bvector) FOR SEARCH;

CREATE OPERATOR CLASS bvector_jaccard_ops
    FOR TYPE bvector USING vectors FAMILY bvector_jaccard_ops AS
    OPERATOR 1 <~> (bvector, bvector) FOR ORDER BY float_ops,
    OPERATOR 2 <<~>> (bvector, sphere_bvector) FOR SEARCH;

-- List of views

CREATE VIEW pg_vector_index_stat AS
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
    WHERE A.amname = 'vectors' AND C.relkind = 'r';

GRANT SELECT ON TABLE pg_vector_index_stat TO PUBLIC;

-- finalize end
