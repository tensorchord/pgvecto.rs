use std::time::Duration;

use crate::datatype::vecf32::{Vecf32, Vecf32Output};
use crate::gucs::embedding::{openai_options, OpenAIOptions};
use pgrx::error;
use reqwest::blocking::Client;
use service::prelude::F32;

use super::openai::{EmbeddingError, EmbeddingRequest, EmbeddingResponse};

#[pgrx::pg_extern(volatile, strict)]
fn _vectors_ai_embedding_vector_v3(input: String) -> Vecf32Output {
    _vectors_ai_embedding_vector(input, "text-embedding-3-small".to_string())
}

#[pgrx::pg_extern(volatile, strict)]
fn _vectors_ai_embedding_vector(input: String, model: String) -> Vecf32Output {
    let options = openai_options();
    let resp = match openai_embedding(input, model, options) {
        Ok(r) => r,
        Err(e) => error!("{}", e.to_string()),
    };
    let embedding = match resp.try_pop_embedding() {
        Ok(emb) => emb.into_iter().map(F32).collect::<Vec<_>>(),
        Err(e) => error!("{}", e.to_string()),
    };

    Vecf32::new_in_postgres(&embedding)
}

pub fn openai_embedding(
    input: String,
    model: String,
    opt: OpenAIOptions,
) -> Result<EmbeddingResponse, EmbeddingError> {
    let url = format!("{}/embeddings", opt.base_url);
    let client = match Client::builder().timeout(Duration::from_secs(30)).build() {
        Ok(c) => c,
        Err(e) => {
            return Err(EmbeddingError {
                hint: e.to_string(),
            })
        }
    };
    let form: EmbeddingRequest = EmbeddingRequest::new(model.to_string(), input);
    let resp = match client
        .post(url)
        .header("Authorization", format!("Bearer {}", opt.api_key))
        .form(&form)
        .send()
    {
        Ok(c) => c,
        Err(e) => {
            return Err(EmbeddingError {
                hint: e.to_string(),
            })
        }
    };
    match resp.json::<EmbeddingResponse>() {
        Ok(c) => Ok(c),
        Err(e) => Err(EmbeddingError {
            hint: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::embedding::openai::EmbeddingData;
    use crate::embedding::openai::Usage;

    use super::openai_embedding;
    use super::EmbeddingResponse;
    use super::OpenAIOptions;
    use httpmock::Method::POST;
    use httpmock::MockServer;

    fn mock_server(resp: EmbeddingResponse) -> MockServer {
        let server = MockServer::start();
        let data = serde_json::to_string(&resp).unwrap();
        let _ = server.mock(|when, then| {
            when.method(POST).path("/embeddings");
            then.status(200)
                .header("content-type", "text/html; charset=UTF-8")
                .body(data);
        });
        server
    }

    #[test]
    fn test_openai_embedding_successful() {
        let embedding = vec![1.0, 2.0, 3.0];
        let resp = EmbeddingResponse {
            object: "mock-object".to_string(),
            data: vec![EmbeddingData {
                object: "mock-object".to_string(),
                embedding: embedding.clone(),
                index: 0,
            }],
            model: "mock-model".to_string(),
            usage: Usage {
                prompt_tokens: 0,
                total_tokens: 0,
            },
        };
        let server = mock_server(resp);

        let opt = OpenAIOptions {
            base_url: server.url(""),
            api_key: "fake-key".to_string(),
        };

        let real_resp = openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt);
        assert!(real_resp.is_ok());
        let real_embedding = real_resp.unwrap().try_pop_embedding();
        assert!(real_embedding.is_ok());
    }

    #[test]
    fn test_openai_embedding_empty_embedding() {
        let resp = EmbeddingResponse {
            object: "mock-object".to_string(),
            data: vec![],
            model: "mock-model".to_string(),
            usage: Usage {
                prompt_tokens: 0,
                total_tokens: 0,
            },
        };
        let server = mock_server(resp);

        let opt = OpenAIOptions {
            base_url: server.url(""),
            api_key: "fake-key".to_string(),
        };

        let real_resp = openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt);
        assert!(real_resp.is_ok());
        let real_embedding = real_resp.unwrap().try_pop_embedding();
        assert!(real_embedding.is_err());
    }

    #[test]
    fn test_openai_embedding_error() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(POST).path("/embeddings");
            then.status(502)
                .header("content-type", "text/html; charset=UTF-8")
                .body("502 Bad Gateway");
        });

        let opt = OpenAIOptions {
            base_url: server.url(""),
            api_key: "fake-key".to_string(),
        };

        let real_resp = openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt);
        assert!(real_resp.is_err());
    }
}
