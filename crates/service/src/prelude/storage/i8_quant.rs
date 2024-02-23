use crate::algorithms::raw::RawRam;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::mmap_array::MmapArray;
use std::path::Path;

pub struct I8QuantMmap {
    vectors: MmapArray<I8>,
    alphas: MmapArray<F32>,
    offsets: MmapArray<F32>,
    sums: MmapArray<F32>,
    l2_norms: MmapArray<F32>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl Storage for I8QuantMmap {
    type VectorRef<'a> = VecI8Ref<'a>;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> VecI8Ref<'_> {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        VecI8Ref {
            dims: self.dims,
            data: &self.vectors[s..e],
            alpha: self.alphas[i as usize],
            offset: self.offsets[i as usize],
            sum: self.sums[i as usize],
            l2_norm: self.l2_norms[i as usize],
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
        let alphas = MmapArray::open(&path.join("alphas"));
        let offsets = MmapArray::open(&path.join("offsets"));
        let sums = MmapArray::open(&path.join("sums"));
        let l2_norms = MmapArray::open(&path.join("l2_norms"));
        let payload = MmapArray::open(&path.join("payload"));
        Self {
            vectors,
            alphas,
            offsets,
            sums,
            l2_norms,
            payload,
            dims: options.vector.dims,
        }
    }

    fn save<S: for<'a> G<VectorRef<'a> = Self::VectorRef<'a>>>(
        path: &Path,
        ram: RawRam<S>,
    ) -> Self {
        let n = ram.len();
        let vectors_iter = (0..n).flat_map(|i| ram.vector(i).data.iter().copied());
        let alphas_iter = (0..n).map(|i| ram.vector(i).alpha);
        let offsets_iter = (0..n).map(|i| ram.vector(i).offset);
        let sums_iter = (0..n).map(|i| ram.vector(i).sum);
        let l2_norms_iter = (0..n).map(|i| ram.vector(i).l2_norm);
        let payload_iter = (0..n).map(|i| ram.payload(i));
        let vectors = MmapArray::create(&path.join("vectors"), vectors_iter);
        let alphas = MmapArray::create(&path.join("alphas"), alphas_iter);
        let offsets = MmapArray::create(&path.join("offsets"), offsets_iter);
        let sums = MmapArray::create(&path.join("sums"), sums_iter);
        let l2_norms = MmapArray::create(&path.join("l2_norms"), l2_norms_iter);
        let payload = MmapArray::create(&path.join("payload"), payload_iter);
        Self {
            vectors,
            alphas,
            offsets,
            sums,
            l2_norms,
            payload,
            dims: ram.dims(),
        }
    }
}
