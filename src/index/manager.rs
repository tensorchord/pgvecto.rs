use std::{collections::HashMap, fmt::Debug};

use once_cell::sync::OnceCell;
use pgrx::prelude::*;

#[derive(Debug)]
pub(crate) enum IndexType {
    HNSW,
    IVFPQ,
}

pub type Vector = Vec<f32>;

trait IndexEngine: Debug + Sync + Send {
    type Item;

    fn insert(&mut self, point: Self::Item) -> bool;
    fn search(&self, point: Self::Item, top_k: i32) -> Option<Self::Item>;
    fn build(&mut self, points: Vec<Self::Item>) -> bool;
    fn distance(&self, x: Self::Item, y: Self::Item) -> f64;
}

#[derive(Debug)]
pub(crate) struct VectorsIndexContext {
    index_type: IndexType,
    engine: Box<dyn IndexEngine<Item = Vector>>,
}

#[derive(Debug)]
pub(crate) struct VectorsManager {
    context: Option<HashMap<pg_sys::Oid, VectorsIndexContext>>,
}

pub(crate) fn global_vectors_manager() -> &'static VectorsManager {
    static VECTORS_MANAGER: OnceCell<VectorsManager> = OnceCell::new();
    VECTORS_MANAGER.get_or_init(|| VectorsManager { context: None })
}
