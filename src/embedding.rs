use openai_api_rust::{
    embeddings::{EmbeddingsApi, EmbeddingsBody},
    Auth, OpenAI,
};

pub(crate) type Embedding = Vec<f64>;
pub(crate) type Embeddings = Vec<Embedding>;

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
        Self::new(api_key, EmbeddingModel::Ada002)
    }

    pub(crate) fn new(api_key: String, model: EmbeddingModel) -> Self {
        Self {
            api_key,
            model,
            api_url: OPENAI_API_URL_BASE.to_string(),
        }
    }

    pub(crate) fn create_embeddings(&self, input: Vec<String>) -> Result<Embeddings, String> {
        let auth = Auth::new(&self.api_key);
        let client = OpenAI::new(auth, &self.api_url);

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
