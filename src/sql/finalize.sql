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
