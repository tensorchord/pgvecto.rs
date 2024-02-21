use crate::datatype::vecf32::{Vecf32, Vecf32Output};
use crate::gucs::embedding::openai_options;
use embedding::openai_embedding;
use pgrx::error;
use service::prelude::F32;

#[pgrx::pg_extern(volatile, strict)]
fn _vectors_text2vec_openai(input: String, model: String) -> Vecf32Output {
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
