pub mod openai;

use crate::openai::EmbeddingError;
use crate::openai::EmbeddingRequest;
use crate::openai::EmbeddingResponse;
use reqwest::blocking::Client;
use std::time::Duration;

pub struct OpenAIOptions {
    pub base_url: String,
    pub api_key: String,
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
    use crate::openai::EmbeddingData;
    use crate::openai::Usage;

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
