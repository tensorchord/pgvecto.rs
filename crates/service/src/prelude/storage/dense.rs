use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::mmap_array::MmapArray;
use std::borrow::Cow;
use std::path::Path;

pub struct DenseMmap<T> {
    vectors: MmapArray<T>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl<T> Storage for DenseMmap<T>
where
    T: Copy + bytemuck::Pod,
{
    type Element = T;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn content(&self, i: u32) -> &[Self::Element] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
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
        let payload = MmapArray::open(&path.join("payload"));
        Self {
            vectors,
            payload,
            dims: options.vector.dims,
        }
    }
}

impl<T> AtomicStorage for DenseMmap<T>
where
    T: Copy + bytemuck::Pod,
{
    type Scalar = T;

    fn check_dims(dims: u16, vector: &[Self::Element]) -> bool {
        vector.len() == dims as usize
    }

    fn vector(_: u16, contents: &[Self::Element]) -> Cow<'_, [Self::Scalar]> {
        Cow::Borrowed(contents)
    }

    fn save(path: &Path, ram: impl Storage<Element = Self::Element>) -> Self
    where
        Self: Sized,
    {
        let n = ram.len();
        let vectors_iter = (0..n).flat_map(|i| ram.content(i)).copied();
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
