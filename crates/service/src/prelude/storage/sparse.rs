use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::mmap_array::MmapArray;
use std::path::Path;

pub struct SparseMmap {
    vectors: MmapArray<SparseF32Element>,
    offsets: MmapArray<u32>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl Storage for SparseMmap {
    type Element = SparseF32Element;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn content(&self, i: u32) -> &[SparseF32Element] {
        let s = self.offsets[i as usize] as usize;
        let e = self.offsets[i as usize + 1] as usize;
        &self.vectors[s..e]
    }

    fn payload(&self, i: u32) -> Payload {
        self.payload[i as usize]
    }

    fn load(path: &Path, options: IndexOptions) -> Self
    where
        Self: Sized,
    {
        let vectors = MmapArray::open(&path.join("vectors"));
        let offsets = MmapArray::open(&path.join("offsets"));
        let payload = MmapArray::open(&path.join("payload"));
        Self {
            vectors,
            offsets,
            payload,
            dims: options.vector.dims,
        }
    }

    fn save(path: &Path, ram: impl Ram<Element = Self::Element>) -> Self
    where
        Self: Sized,
    {
        let n = ram.len();
        let vectors_iter = (0..n).flat_map(|i| ram.content(i)).copied();
        let offsets_iter = std::iter::once(0)
            .chain((0..n).map(|i| ram.content(i).len() as u32))
            .scan(0, |state, x| {
                *state += x;
                Some(*state)
            });
        let payload_iter = (0..n).map(|i| ram.payload(i));
        let vectors = MmapArray::create(&path.join("vectors"), vectors_iter);
        let offsets = MmapArray::create(&path.join("offsets"), offsets_iter);
        let payload = MmapArray::create(&path.join("payload"), payload_iter);
        Self {
            vectors,
            offsets,
            payload,
            dims: ram.dims(),
        }
    }
}
