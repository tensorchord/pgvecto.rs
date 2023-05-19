CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING pgvectors_hnsw AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 operator_l2_distance(vector, vector);
