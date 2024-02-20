use crate::algorithms::raw::RawRam;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::mmap_array::MmapArray;
use bitvec::view::BitView;
use std::path::Path;

pub struct BinaryMmap {
    vectors: MmapArray<usize>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl Storage for BinaryMmap {
    type VectorRef<'a> = BinaryVecRef<'a>;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> BinaryVecRef<'_> {
        let bit_size = (self.dims as usize).div_ceil(std::mem::size_of::<usize>() * 8);
        let s = i as usize * bit_size;
        let e = (i + 1) as usize * bit_size;
        BinaryVecRef {
            values: &self.vectors[s..e].view_bits()[..self.dims as usize],
        }
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
            dims: options.vector.dims,
        }
    }

    fn save<S: for<'a> G<VectorRef<'a> = Self::VectorRef<'a>>>(
        path: &Path,
        ram: RawRam<S>,
    ) -> Self {
        let n = ram.len();
        let vectors_iter = (0..n)
            .flat_map(|i| ram.vector(i).as_bytes().iter())
            .copied();
        let payload_iter = (0..n).map(|i| ram.payload(i));
        let vectors = MmapArray::create(&path.join("vectors"), vectors_iter);
        let payload = MmapArray::create(&path.join("payload"), payload_iter);
        Self {
            vectors,
            payload,
            dims: ram.dims(),
        }
    }
}
