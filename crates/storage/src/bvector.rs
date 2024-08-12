use crate::Storage;
use base::operator::Operator;
use base::search::*;
use base::vector::*;
use common::json::Json;
use common::mmap_array::MmapArray;
use std::path::Path;

pub struct BVectorStorage {
    dims: Json<u32>,
    len: Json<u32>,
    slice: MmapArray<u64>,
}

impl<O: Operator<VectorOwned = BVectorOwned>> Vectors<O> for BVectorStorage {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> BVectorBorrowed<'_> {
        let w = self.dims.div_ceil(BVECTOR_WIDTH);
        let s = i as usize * w as usize;
        let e = (i + 1) as usize * w as usize;
        BVectorBorrowed::new(*self.dims as _, &self.slice[s..e])
    }
}

impl<O: Operator<VectorOwned = BVectorOwned>> Storage<O> for BVectorStorage {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<O>) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let dims = Json::create(path.as_ref().join("dims"), vectors.dims());
        let len = Json::create(path.as_ref().join("len"), vectors.len());
        let slice = MmapArray::create(
            path.as_ref().join("slice"),
            (0..*len).flat_map(|i| vectors.vector(i).data().iter().copied()),
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
