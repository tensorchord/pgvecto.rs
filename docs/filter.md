# Filter in vector search

It's common to want to filter a vector search by some criteria. For example, you might want to filter by a certain category like `select * from items WHERE category = 1 ORDER BY embedding <-> '[3,2,1]' LIMIT 100`. However, there're several possible execution plan for this kind of query. Currently pgvecto.rs use prefilter by default, and enable user to choose postfilter or brute-force if needed.

The actual performance of filtering depends on how tight your condition is:
- If your filter is loose, let's say 90% data satisfy it. Performing an ANN search first and then applying a filter to the results would yield the fastest and relatively good outcomes. This is post filtering
- If your filter is really tight, let's say only 100 rows satisfy it. The optimal approach is to apply the filter first, obtaining 100 results, and then calculate the distance directly without utilizing any vector index. This is brute force
- If your filter is kind of tight, let's say 20% data satisfy it. The post-filtering strategy may encounter issues as the ANN search might not yield sufficient results for filtering. The optimal approach in this case is prefiltering. As the algorithm traverses the hnsw graph to discover new points, it will simultaneously verify the filter until there are an adequate number of candidates. All the results from the 
vector index have already met the filter criteria.

To select different mode:
Prefiltering (default mode): SET vectors.enable_vector_index=on; SET vectors.enable_prefilter=on
Postfiltering: SET vectors.enable_vector_index=on; SET vectors.enable_prefilter=off
Brute force: SET vectors.enable_vector_index=off
