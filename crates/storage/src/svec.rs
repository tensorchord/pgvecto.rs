use crate::Storage;
use base::operator::Operator;
use base::scalar::*;
use base::search::*;
use base::vector::*;
use common::json::Json;
use common::mmap_array::MmapArray;
use std::path::Path;

pub struct SVecStorage {
    dims: Json<u32>,
    len: Json<u32>,
    indexes: MmapArray<u32>,
    values: MmapArray<F32>,
    offsets: MmapArray<usize>,
}

impl<O: Operator<VectorOwned = SVecf32Owned>> Vectors<O> for SVecStorage {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> SVecf32Borrowed<'_> {
        let s = self.offsets[i as usize];
        let e = self.offsets[i as usize + 1];
        unsafe {
            SVecf32Borrowed::new_unchecked(*self.dims, &self.indexes[s..e], &self.values[s..e])
        }
    }
}

impl<O: Operator<VectorOwned = SVecf32Owned>> Storage<O> for SVecStorage {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<O>) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let dims = Json::create(path.as_ref().join("dims"), vectors.dims());
        let len = Json::create(path.as_ref().join("len"), vectors.len());
        let indexes = MmapArray::create(
            path.as_ref().join("indexes"),
            (0..*len).flat_map(|i| vectors.vector(i).indexes().to_vec()),
        );
        let values = MmapArray::create(
            path.as_ref().join("values"),
            (0..*len).flat_map(|i| vectors.vector(i).values().to_vec()),
        );
        let offsets = MmapArray::create(
            path.as_ref().join("offsets"),
            std::iter::once(0)
                .chain((0..*len).map(|i| vectors.vector(i).len() as usize))
                .scan(0, |state, x| {
                    *state += x;
                    Some(*state)
                }),
        );
        Self {
            dims,
            len,
            indexes,
            values,
            offsets,
        }
    }

    fn open(path: impl AsRef<Path>) -> Self {
        let dims = Json::open(path.as_ref().join("dims"));
        let len = Json::open(path.as_ref().join("len"));
        let indexes = MmapArray::open(path.as_ref().join("indexes"));
        let values = MmapArray::open(path.as_ref().join("values"));
        let offsets = MmapArray::open(path.as_ref().join("offsets"));
        Self {
            dims,
            len,
            indexes,
            values,
            offsets,
        }
    }
}
