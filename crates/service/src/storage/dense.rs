use crate::algorithms::raw::RawRam;
use crate::prelude::*;
use crate::storage::Storage;
use crate::utils::mmap_array::MmapArray;
use std::path::Path;

pub struct DenseMmap<T> {
    vectors: MmapArray<T>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl Storage for DenseMmap<F32> {
    type VectorOwned = Vecf32Owned;

    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> Vecf32Borrowed<'_> {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        Vecf32Borrowed::new(&self.vectors[s..e])
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
        let vectors_iter = (0..n).flat_map(|i| ram.vector(i).to_vec());
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

impl Storage for DenseMmap<F16> {
    type VectorOwned = Vecf16Owned;

    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> Vecf16Borrowed {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        Vecf16Borrowed::new(&self.vectors[s..e])
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
        let vectors_iter = (0..n).flat_map(|i| ram.vector(i).to_vec());
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
