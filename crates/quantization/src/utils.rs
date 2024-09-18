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

pub fn merge_8([b0, b1, b2, b3, b4, b5, b6, b7]: [u8; 8]) -> u8 {
    b0 | (b1 << 1) | (b2 << 2) | (b3 << 3) | (b4 << 4) | (b5 << 5) | (b6 << 6) | (b7 << 7)
}

pub fn merge_4([b0, b1, b2, b3]: [u8; 4]) -> u8 {
    b0 | (b1 << 2) | (b2 << 4) | (b3 << 6)
}

pub fn merge_2([b0, b1]: [u8; 2]) -> u8 {
    b0 | (b1 << 4)
}
