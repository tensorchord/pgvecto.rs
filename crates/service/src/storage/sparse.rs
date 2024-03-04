use crate::algorithms::raw::RawRam;
use crate::prelude::*;
use crate::storage::Storage;
use crate::utils::mmap_array::MmapArray;
use std::path::Path;

pub struct SparseMmap {
    indexes: MmapArray<u32>,
    values: MmapArray<F32>,
    offsets: MmapArray<usize>,
    payload: MmapArray<Payload>,
    dims: u32,
}

impl Storage for SparseMmap {
    type VectorOwned = SVecf32Owned;

    fn dims(&self) -> u32 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> SVecf32Borrowed<'_> {
        let s = self.offsets[i as usize];
        let e = self.offsets[i as usize + 1];
        unsafe {
            SVecf32Borrowed::new_unchecked(self.dims, &self.indexes[s..e], &self.values[s..e])
        }
    }

    fn payload(&self, i: u32) -> Payload {
        self.payload[i as usize]
    }

    fn open(path: &Path, options: IndexOptions) -> Self
    where
        Self: Sized,
    {
        let indexes = MmapArray::open(&path.join("indexes"));
        let values = MmapArray::open(&path.join("values"));
        let offsets = MmapArray::open(&path.join("offsets"));
        let payload = MmapArray::open(&path.join("payload"));
        Self {
            indexes,
            values,
            offsets,
            payload,
            dims: options.vector.dims,
        }
    }

    fn save<S: G<VectorOwned = SVecf32Owned>>(path: &Path, ram: RawRam<S>) -> Self {
        let n = ram.len();
        let indexes_iter = (0..n).flat_map(|i| ram.vector(i).indexes().to_vec());
        let values_iter = (0..n).flat_map(|i| ram.vector(i).values().to_vec());
        let offsets_iter = std::iter::once(0)
            .chain((0..n).map(|i| ram.vector(i).len() as usize))
            .scan(0, |state, x| {
                *state += x;
                Some(*state)
            });
        let payload_iter = (0..n).map(|i| ram.payload(i));
        let indexes = MmapArray::create(&path.join("indexes"), indexes_iter);
        let values = MmapArray::create(&path.join("values"), values_iter);
        let offsets = MmapArray::create(&path.join("offsets"), offsets_iter);
        let payload = MmapArray::create(&path.join("payload"), payload_iter);
        Self {
            indexes,
            values,
            offsets,
            payload,
            dims: ram.dims(),
        }
    }
}
