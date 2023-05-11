use pgrx::prelude::*;

use crate::{
    embedding::{Embedding, OpenAIEmbedding},
    gucs::OPENAI_API_KEY_GUC,
};

#[pg_extern(immutable)]
fn ai_embedding_vector(input: String) -> Embedding {
    let api_key = match OPENAI_API_KEY_GUC.get() {
        Some(key) => key,
        None => {
            error!("openai_api_key is not set");
        }
    };

    // default to use ada002
    let openai_embedding = OpenAIEmbedding::new_ada002(api_key);

    match openai_embedding.create_embeddings(vec![input]) {
        Ok(mut embeddings) => match embeddings.pop() {
            Some(embedding) => embedding,
            None => {
                error!("embedding is empty")
            }
        },
        Err(e) => {
            error!("failed to create embedding, {}", e)
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    // We need to mock embedding requests since it requires an API key.
    #[pg_test]
    fn test_ai_embedding_vector() {}
}
