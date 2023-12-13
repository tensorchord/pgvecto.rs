# Why not a specialty vector database?

Read our complete blog at [modelz.ai/blog/pgvector](https://modelz.ai/blog/pgvector)

Imagine this, your existing data is stored in a Postgres database, and you want to use a vector database to do some vector similarity search. You have to move your data from Postgres to the vector database, and you have to maintain two databases at the same time. This is not a good idea.

Why not just use Postgres to do the vector similarity search? This is the reason why we build pgvecto.rs. The user journey is like this:

```sql
-- Update the embedding column for the documents table
UPDATE documents SET embedding = ai_embedding_vector(content) WHERE length(embedding) = 0;

-- Create an index on the embedding column
CREATE INDEX ON documents USING vectors (embedding vector_l2_ops);

-- Query the similar embeddings
SELECT * FROM documents ORDER BY embedding <-> ai_embedding_vector('hello world') LIMIT 5;
```

From [SingleStore DB Blog](https://www.singlestore.com/blog/why-your-vector-database-should-not-be-a-vector-database/):

> Vectors and vector search are a data type and query processing approach, not a foundation for a new way of processing data. Using a specialty vector database (SVDB) will lead to the usual problems we see (and solve) again and again with our customers who use multiple specialty systems: redundant data, excessive data movement, lack of agreement on data values among distributed components, extra labor expense for specialized skills, extra licensing costs, limited query language power, programmability and extensibility, limited tool integration, and poor data integrity and availability compared with a true DBMS.
