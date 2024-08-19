use crate::Storage;
use base::scalar::*;
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

impl Vectors<Vecf32Owned> for VecStorage<F32> {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> Vecf32Borrowed<'_> {
        let s = i as usize * *self.dims as usize;
        let e = (i + 1) as usize * *self.dims as usize;
        Vecf32Borrowed::new(&self.slice[s..e])
    }
}

impl Storage<Vecf32Owned> for VecStorage<F32> {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<Vecf32Owned>) -> Self {
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

impl Vectors<Vecf16Owned> for VecStorage<F16> {
    fn dims(&self) -> u32 {
        *self.dims
    }

    fn len(&self) -> u32 {
        *self.len
    }

    fn vector(&self, i: u32) -> Vecf16Borrowed {
        let s = i as usize * *self.dims as usize;
        let e = (i + 1) as usize * *self.dims as usize;
        Vecf16Borrowed::new(&self.slice[s..e])
    }
}

impl Storage<Vecf16Owned> for VecStorage<F16> {
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<Vecf16Owned>) -> Self {
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
