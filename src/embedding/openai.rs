use crate::prelude::embedding_failed;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Deserialize, Serialize)]
pub struct EmbeddingData {
    pub object: String,
    pub embedding: Vec<f32>,
    pub index: i32,
}

#[derive(Debug, Serialize, Clone)]
pub struct EmbeddingRequest {
    pub model: String,
    pub input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

impl EmbeddingRequest {
    pub fn new(model: String, input: String) -> Self {
        Self {
            model,
            input,
            dimensions: None,
            user: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EmbeddingResponse {
    pub object: String,
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: Usage,
}

impl EmbeddingResponse {
    pub fn pop_embedding(mut self) -> Vec<f32> {
        match self.data.pop() {
            Some(d) => d.embedding,
            None => embedding_failed("no embedding from service"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Usage {
    pub prompt_tokens: i32,
    pub total_tokens: i32,
}
