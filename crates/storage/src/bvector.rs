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
    slice: MmapArray<usize>,
}

impl<O: Operator<VectorOwned = BVecf32Owned>> Vectors<O> for BVectorStorage {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> BVecf32Borrowed<'_> {
        let size = (*self.dims as usize).div_ceil(BVEC_WIDTH);
        let s = i as usize * size;
        let e = (i + 1) as usize * size;
        BVecf32Borrowed::new(*self.dims as _, &self.slice[s..e])
    }
}

impl<O: Operator<VectorOwned = BVecf32Owned>> Storage<O> for BVectorStorage {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<O>) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let dims = Json::create(path.as_ref().join("dims"), vectors.dims());
        let len = Json::create(path.as_ref().join("len"), vectors.len());
        let slice = MmapArray::create(
            path.as_ref().join("slice"),
            (0..*len).flat_map(|i| vectors.vector(i).data().iter().copied()),
        );
        common::dir_ops::sync_dir(path);
        Self { dims, len, slice }
    }

    fn open(path: impl AsRef<Path>) -> Self {
        let dims = Json::open(path.as_ref().join("dims"));
        let len = Json::open(path.as_ref().join("len"));
        let slice = MmapArray::open(path.as_ref().join("slice"));
        Self { dims, len, slice }
    }
}
