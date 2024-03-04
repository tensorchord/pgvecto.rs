use crate::algorithms::raw::RawRam;
use crate::prelude::*;
use crate::storage::Storage;
use crate::utils::mmap_array::MmapArray;
use std::path::Path;

pub struct BinaryMmap {
    vectors: MmapArray<usize>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl Storage for BinaryMmap {
    type VectorOwned = BVecf32Owned;

    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> BVecf32Borrowed<'_> {
        let size = (self.dims as usize).div_ceil(BVEC_WIDTH);
        let s = i as usize * size;
        let e = (i + 1) as usize * size;
        BVecf32Borrowed::new(self.dims, &self.vectors[s..e])
    }

    fn payload(&self, i: u32) -> Payload {
        self.payload[i as usize]
    }

    fn open(path: &Path, options: IndexOptions) -> Self
    where
        Self: Sized,
    {
        let vectors = MmapArray::open(&path.join("vectors"));
        let payload = MmapArray::open(&path.join("payload"));
        Self {
            vectors,
            payload,
            dims: options.vector.dims.try_into().unwrap(),
        }
    }

    fn save<S: G<VectorOwned = Self::VectorOwned>>(path: &Path, ram: RawRam<S>) -> Self {
        let n = ram.len();
        let vectors_iter = (0..n).flat_map(|i| ram.vector(i).data().iter().copied());
        let payload_iter = (0..n).map(|i| ram.payload(i));
        let vectors = MmapArray::create(&path.join("vectors"), vectors_iter);
        let payload = MmapArray::create(&path.join("payload"), payload_iter);
        Self {
            vectors,
            payload,
            dims: ram.dims().try_into().unwrap(),
        }
    }
}
