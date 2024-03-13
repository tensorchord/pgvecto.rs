use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::Collection;
use common::dir_ops::sync_dir;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub struct TrivialQuantization<O: Operator, C: Collection<O>> {
    collection: Arc<C>,
    permutation: Vec<u32>,
    _maker: PhantomData<fn(O) -> O>,
}

impl<O: Operator, C: Collection<O>> TrivialQuantization<O, C> {
    fn codes(&self, i: u32) -> Borrowed<'_, O> {
        self.collection.vector(self.permutation[i as usize])
    }
}

impl<O: Operator, C: Collection<O>> TrivialQuantization<O, C> {
    // permutation is the mapping from placements to original ids
    pub fn create(
        path: &Path,
        _: IndexOptions,
        _: QuantizationOptions,
        collection: &Arc<C>,
        permutation: Vec<u32>,
    ) -> Self {
        // here we cannot modify origin, so we record permutation for translation
        std::fs::create_dir(path).unwrap();
        sync_dir(path);
        std::fs::write(
            path.join("permutation"),
            serde_json::to_string(&permutation).unwrap(),
        )
        .unwrap();
        Self {
            collection: collection.clone(),
            permutation,
            _maker: PhantomData,
        }
    }

    pub fn open(path: &Path, _: IndexOptions, _: QuantizationOptions, collection: &Arc<C>) -> Self {
        let permutation =
            serde_json::from_slice(&std::fs::read(path.join("permutation")).unwrap()).unwrap();
        Self {
            collection: collection.clone(),
            permutation,
            _maker: PhantomData,
        }
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: u32) -> F32 {
        O::distance(lhs, self.codes(rhs))
    }

    pub fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        O::distance(self.codes(lhs), self.codes(rhs))
    }
}
