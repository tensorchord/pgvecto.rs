use pgrx::prelude::*;

use crate::{
    embedding::{Embedding, EmbeddingCreator, OpenAIEmbedding},
    postgres::gucs::OPENAI_API_KEY_GUC,
};

#[pg_extern]
fn ai_embedding_vector(input: String) -> Embedding {
    let api_key = match OPENAI_API_KEY_GUC.get() {
        Some(key) => key,
        None => {
            error!("openai_api_key is not set");
        }
    };

    // default to use ada002
    let openai_embedding = OpenAIEmbedding::new_ada002(api_key);

    match ai_embedding_vector_inner(input, openai_embedding) {
        Ok(embedding) => embedding,
        Err(e) => {
            error!("{}", e)
        }
    }
}

fn ai_embedding_vector_inner(
    input: String,
    client: impl EmbeddingCreator,
) -> Result<Embedding, String> {
    match client.create_embeddings(vec![input]) {
        Ok(mut embeddings) => match embeddings.pop() {
            Some(embedding) => Ok(embedding),
            None => Err("embedding is empty".to_string()),
        },
        Err(e) => Err(format!("failed to create embedding, {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use crate::embedding::MockEmbeddingCreator;
    use crate::udf::ai_embedding_vector_inner;
    use mockall::predicate::eq;

    // We need to mock embedding since it requires an API key.
    #[test]
    fn test_ai_embedding_vector_inner_successful() {
        let input = String::from("input");
        let mut mock_client = MockEmbeddingCreator::new();
        let expected_embedding = vec![1.0, 2.0, 3.0];
        mock_client
            .expect_create_embeddings()
            .with(eq(vec![input.clone()]))
            .returning(|_| Ok(vec![vec![1.0, 2.0, 3.0]]));

        let result = ai_embedding_vector_inner(input, mock_client);

        assert_eq!(result, Ok(expected_embedding));
    }

    #[test]
    fn test_ai_embedding_vector_inner_empty_embedding() {
        let input = String::from("input");
        let mut mock_client = MockEmbeddingCreator::new();
        mock_client
            .expect_create_embeddings()
            .with(eq(vec![input.clone()]))
            .returning(|_| Ok(vec![]));

        let result = ai_embedding_vector_inner(input, mock_client);
        assert_eq!(result, Err("embedding is empty".to_string()))
    }

    #[test]
    fn test_ai_embedding_vector_inner_error() {
        let input = String::from("input");
        let mut mock_client = MockEmbeddingCreator::new();
        mock_client
            .expect_create_embeddings()
            .with(eq(vec![input.clone()]))
            .returning(|_| Err(String::from("invalid input")));

        let result = ai_embedding_vector_inner(input, mock_client);

        assert_eq!(
            result,
            Err("failed to create embedding, invalid input".to_string())
        )
    }
}
