use std::time::Duration;

use crate::datatype::vecf32::{Vecf32, Vecf32Output};
use crate::gucs::embedding::{openai_options, OpenAIOptions};
use crate::prelude::embedding_failed;
use pgrx::default;
use reqwest::blocking::Client;
use service::prelude::F32;

use super::openai::{EmbeddingRequest, EmbeddingResponse};

#[pgrx::pg_extern(volatile, strict)]
fn _vectors_ai_embedding_vector(
    input: String,
    model: default!(String, "'text-embedding-ada-002'"),
) -> Vecf32Output {
    let options = openai_options();
    let embedding = openai_embedding(input, model, options)
        .pop_embedding()
        .into_iter()
        .map(F32)
        .collect::<Vec<_>>();
    Vecf32::new_in_postgres(&embedding)
}

pub fn openai_embedding(input: String, model: String, opt: OpenAIOptions) -> EmbeddingResponse {
    let url = format!("{}/embeddings", opt.base_url);
    let client = match Client::builder().timeout(Duration::from_secs(30)).build() {
        Ok(c) => c,
        Err(e) => embedding_failed(&e.to_string()),
    };
    let form: EmbeddingRequest = EmbeddingRequest::new(model.to_string(), input);
    let resp = match client
        .post(url)
        .header("Authorization", format!("Bearer {}", opt.api_key))
        .form(&form)
        .send()
    {
        Ok(c) => c,
        Err(e) => embedding_failed(&e.to_string()),
    };
    match resp.json::<EmbeddingResponse>() {
        Ok(c) => c,
        Err(e) => embedding_failed(&e.to_string()),
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
        let real_embedding = real_resp.pop_embedding();
        assert_eq!(real_embedding, embedding);
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
        let expect_panic = std::panic::catch_unwind(|| real_resp.pop_embedding());
        assert!(expect_panic.is_err());
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

        let real_resp = std::panic::catch_unwind(|| {
            openai_embedding("mock-input".to_string(), "mock-model".to_string(), opt)
        });
        assert!(real_resp.is_err());
    }
}
