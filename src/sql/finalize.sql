CREATE OPERATOR CLASS l2_ops
	FOR TYPE vector USING pgvectors AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS dot_ops
	FOR TYPE vector USING pgvectors AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops;

CREATE OPERATOR CLASS cosine_ops
	FOR TYPE vector USING pgvectors AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops;
