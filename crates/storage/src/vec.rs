use crate::Storage;
use base::scalar::ScalarLike;
use base::search::*;
use base::vector::*;
use common::json::Json;
use common::mmap_array::MmapArray;
use std::path::Path;

pub struct VecStorage<T> {
    dims: Json<u32>,
    len: Json<u32>,
    slice: MmapArray<T>,
}

impl<S: ScalarLike> Vectors<VectOwned<S>> for VecStorage<S> {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> VectBorrowed<'_, S> {
        let s = i as usize * *self.dims as usize;
        let e = (i + 1) as usize * *self.dims as usize;
        VectBorrowed::new(&self.slice[s..e])
    }
}

impl<S: ScalarLike> Storage<VectOwned<S>> for VecStorage<S> {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<VectOwned<S>>) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let dims = Json::create(path.as_ref().join("dims"), vectors.dims());
        let len = Json::create(path.as_ref().join("len"), vectors.len());
        let slice = MmapArray::create(
            path.as_ref().join("slice"),
            (0..*len).flat_map(|i| vectors.vector(i).slice().iter().copied()),
        );
        Self { dims, len, slice }
    }

    fn open(path: impl AsRef<Path>) -> Self {
        let dims = Json::open(path.as_ref().join("dims"));
        let len = Json::open(path.as_ref().join("len"));
        let slice = MmapArray::open(path.as_ref().join("slice"));
        Self { dims, len, slice }
    }
}
