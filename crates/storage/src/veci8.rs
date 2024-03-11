use crate::Storage;
pub use base::index::*;
use base::operator::Operator;
pub use base::scalar::*;
pub use base::search::*;
pub use base::vector::*;
use common::mmap_array::MmapArray;
use std::path::Path;

pub struct Veci8Storage {
    vectors: MmapArray<I8>,
    alphas: MmapArray<F32>,
    offsets: MmapArray<F32>,
    sums: MmapArray<F32>,
    l2_norms: MmapArray<F32>,
    payload: MmapArray<Payload>,
    dims: u32,
}

impl Storage for Veci8Storage {
    type VectorOwned = Veci8Owned;

    fn dims(&self) -> u32 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> Veci8Borrowed<'_> {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        unsafe {
            Veci8Borrowed::new_unchecked(
                self.dims,
                &self.vectors[s..e],
                self.alphas[i as usize],
                self.offsets[i as usize],
                self.sums[i as usize],
                self.l2_norms[i as usize],
            )
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

    fn save<O: Operator<VectorOwned = Veci8Owned>, C: Collection<O>>(
        path: &Path,
        collection: &C,
    ) -> Self {
        let n = collection.len();
        // TODO: how to avoid clone here?
        let vectors_iter = (0..n).flat_map(|i| collection.vector(i).data().to_vec());
        let alphas_iter = (0..n).map(|i| collection.vector(i).alpha());
        let offsets_iter = (0..n).map(|i| collection.vector(i).offset());
        let sums_iter = (0..n).map(|i| collection.vector(i).sum());
        let l2_norms_iter = (0..n).map(|i| collection.vector(i).l2_norm());
        let payload_iter = (0..n).map(|i| collection.payload(i));
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
            dims: collection.dims(),
        }
    }
}
