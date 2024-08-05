#[derive(Debug, Clone)]
pub struct InfiniteByteChunks<I, const N: usize> {
    iter: I,
}

impl<I: Iterator, const N: usize> InfiniteByteChunks<I, N> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: Iterator<Item = u8>, const N: usize> Iterator for InfiniteByteChunks<I, N> {
    type Item = [u8; N];

    fn next(&mut self) -> Option<Self::Item> {
        Some(std::array::from_fn::<u8, N, _>(|_| {
            self.iter.next().unwrap_or(0)
        }))
    }
}
