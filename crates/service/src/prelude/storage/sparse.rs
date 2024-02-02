use crate::algorithms::raw::RawRam;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::mmap_array::MmapArray;
use std::path::Path;

pub struct SparseMmap {
    indexes: MmapArray<u16>,
    values: MmapArray<F32>,
    offsets: MmapArray<u32>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl Storage for SparseMmap {
    type VectorRef<'a> = SparseF32Ref<'a>;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn content(&self, i: u32) -> SparseF32Ref<'_> {
        let s = self.offsets[i as usize] as usize;
        let e = self.offsets[i as usize + 1] as usize;
        SparseF32Ref {
            dims: self.dims,
            indexes: &self.indexes[s..e],
            values: &self.values[s..e],
        }
    }

    fn payload(&self, i: u32) -> Payload {
        self.payload[i as usize]
    }

    fn load(path: &Path, options: IndexOptions) -> Self
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

    fn save<S: for<'a> G<VectorRef<'a> = Self::VectorRef<'a>>>(
        path: &Path,
        ram: RawRam<S>,
    ) -> Self {
        let n = ram.len();
        let indexes_iter = (0..n).flat_map(|i| ram.content(i).indexes.iter().copied());
        let values_iter = (0..n).flat_map(|i| ram.content(i).values.iter().copied());
        let offsets_iter = std::iter::once(0)
            .chain((0..n).map(|i| ram.content(i).length() as u32))
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
