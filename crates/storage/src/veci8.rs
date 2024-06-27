use crate::Storage;
use base::operator::Operator;
use base::scalar::*;
use base::search::*;
use base::vector::*;
use common::json::Json;
use common::mmap_array::MmapArray;
use std::path::Path;

pub struct Veci8Storage {
    dims: Json<u32>,
    len: Json<u32>,
    slice: MmapArray<I8>,
    alphas: MmapArray<F32>,
    offsets: MmapArray<F32>,
    sums: MmapArray<F32>,
    l2_norms: MmapArray<F32>,
}

impl<O: Operator<VectorOwned = Veci8Owned>> Vectors<O> for Veci8Storage {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> Veci8Borrowed<'_> {
        let s = i as usize * *self.dims as usize;
        let e = (i + 1) as usize * *self.dims as usize;
        unsafe {
            Veci8Borrowed::new_unchecked(
                *self.dims,
                &self.slice[s..e],
                self.alphas[i as usize],
                self.offsets[i as usize],
                self.sums[i as usize],
                self.l2_norms[i as usize],
            )
        }
    }
}

impl<O: Operator<VectorOwned = Veci8Owned>> Storage<O> for Veci8Storage {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<O>) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let dims = Json::create(path.as_ref().join("dims"), vectors.dims());
        let len = Json::create(path.as_ref().join("len"), vectors.len());
        let slice = MmapArray::create(
            path.as_ref().join("slice"),
            (0..*len).flat_map(|i| vectors.vector(i).data().to_vec()),
        );
        let alphas = MmapArray::create(
            path.as_ref().join("alphas"),
            (0..*len).map(|i| vectors.vector(i).alpha()),
        );
        let offsets = MmapArray::create(
            path.as_ref().join("offsets"),
            (0..*len).map(|i| vectors.vector(i).offset()),
        );
        let sums = MmapArray::create(
            path.as_ref().join("sums"),
            (0..*len).map(|i| vectors.vector(i).sum()),
        );
        let l2_norms = MmapArray::create(
            path.as_ref().join("l2_norms"),
            (0..*len).map(|i| vectors.vector(i).l2_norm()),
        );
        common::dir_ops::sync_dir(path);
        Self {
            dims,
            len,
            slice,
            alphas,
            offsets,
            sums,
            l2_norms,
        }
    }

    fn open(path: impl AsRef<Path>) -> Self {
        let dims = Json::open(path.as_ref().join("dims"));
        let len = Json::open(path.as_ref().join("len"));
        let slice = MmapArray::open(path.as_ref().join("slice"));
        let alphas = MmapArray::open(path.as_ref().join("alphas"));
        let offsets = MmapArray::open(path.as_ref().join("offsets"));
        let sums = MmapArray::open(path.as_ref().join("sums"));
        let l2_norms = MmapArray::open(path.as_ref().join("l2_norms"));
        Self {
            dims,
            len,
            slice,
            alphas,
            offsets,
            sums,
            l2_norms,
        }
    }
}
