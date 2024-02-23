use crate::datatype::memory_vecf32::Vecf32Output;
use crate::gucs::embedding::openai_options;
use crate::prelude::*;
use embedding::openai_embedding;
use pgrx::error;

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

    Vecf32Output::new(Vecf32Borrowed::new(&embedding))
}
