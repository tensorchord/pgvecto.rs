use openai_api_rust::{
    embeddings::{EmbeddingsApi, EmbeddingsBody},
    Auth, OpenAI,
};

pub(crate) type Embedding = Vec<f64>;
pub(crate) type Embeddings = Vec<Embedding>;

#[cfg(test)]
use mockall::automock;
#[cfg_attr(test, automock)]
pub(crate) trait EmbeddingCreator {
    fn create_embeddings(&self, input: Vec<String>) -> Result<Embeddings, String>;
}

const OPENAI_API_URL_BASE: &str = "https://api.openai.com/v1/";

pub(crate) enum EmbeddingModel {
    // the most used OpenAI embedding model
    Ada002,
}

impl ToString for EmbeddingModel {
    fn to_string(&self) -> String {
        match self {
            Self::Ada002 => "text-embedding-ada-002".to_string(),
        }
    }
}

pub(crate) struct OpenAIEmbedding {
    api_key: String,
    api_url: String,
    model: EmbeddingModel,
}

impl OpenAIEmbedding {
    pub(crate) fn new_ada002(api_key: String) -> Self {
        Self::new(
            api_key,
            EmbeddingModel::Ada002,
            OPENAI_API_URL_BASE.to_string(),
        )
    }

    pub(crate) fn new(api_key: String, model: EmbeddingModel, api_url: String) -> Self {
        Self {
            api_key,
            model,
            api_url,
        }
    }
}

impl EmbeddingCreator for OpenAIEmbedding {
    fn create_embeddings(&self, input: Vec<String>) -> Result<Embeddings, String> {
        let auth = Auth::new(&self.api_key);
        let client = OpenAI::new(auth, &self.api_url).use_env_proxy();

        let embeddings = client
            .embeddings_create(&EmbeddingsBody {
                model: self.model.to_string(),
                input,
                user: None,
            })
            .map_err(|e| e.to_string())?;

        let data: Embeddings = embeddings
            .data
            .unwrap_or_default()
            .into_iter()
            .map(|data| data.embedding.unwrap_or_default())
            .collect();

        Ok(data)
    }
}
#[cfg(test)]
mod tests {
    use httpmock::MockServer;
    use serde_json::json;

    use crate::embedding::{Embedding, EmbeddingCreator};

    use super::OpenAIEmbedding;

    #[test]
    fn test_create_embeddings() {
        let input: String = "hello".to_string();
        let expected_emb: Embedding = vec![0.151240, 0.1231224123, 0.231253124, 0.213125];
        let server = MockServer::start();

        let emb_mock = server.mock(|when, then| {
            when.path("/embeddings");
            then.json_body(format_output_json(expected_emb.clone()));
        });

        let client = OpenAIEmbedding::new(
            "".to_string(),
            crate::embedding::EmbeddingModel::Ada002,
            server.base_url() + "/",
        );

        let mut embs = client.create_embeddings(vec![input]).unwrap();

        let emb = embs.pop().unwrap();
        assert_eq!(emb, expected_emb);
        emb_mock.assert();
    }

    fn format_output_json(embedding: Embedding) -> serde_json::Value {
        json!({
          "data": [
            {
              "embedding": embedding,
              "index": 0,
              "object": "embedding"
            }
          ],
          "model": "text-embedding-ada-002",
          "object": "list",
          "usage": {
            "prompt_tokens": 0,
            "total_tokens": 0
          }
        })
    }
}
