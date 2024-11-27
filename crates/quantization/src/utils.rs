use base::scalar::ScalarLike;

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

pub fn find_scale<S: ScalarLike>(bits: usize, o: &[S]) -> f64 {
    assert!((1..=8).contains(&bits));

    let mask = (1_u32 << (bits - 1)) - 1;
    let dims = o.len();

    let mut code = Vec::<u8>::with_capacity(dims);
    let mut numerator = 0.0f64;
    let mut sqr_denominator = 0.0f64;

    let (mut y_m, mut x_m);

    for i in 0..dims {
        code.push(0);
        numerator += 0.5 * o[i].to_f32() as f64;
        sqr_denominator += 0.5 * 0.5;
    }
    {
        let x = 0.0;
        let y = numerator / sqr_denominator.sqrt();
        (y_m, x_m) = (y, x);
    }

    let mut events = Vec::<(f64, usize)>::new();
    for i in 0..dims {
        for c in 1..=mask {
            let x = (c as f64) / o[i].to_f32() as f64;
            events.push((x, i));
        }
    }
    events.sort_unstable_by(|(x, _), (y, _)| f64::total_cmp(x, y));
    for (x, i) in events.into_iter() {
        code[i] += 1;
        numerator += o[i].to_f32() as f64;
        sqr_denominator += 2.0 * code[i] as f64;

        let y = numerator / sqr_denominator.sqrt();
        if y > y_m {
            (y_m, x_m) = (y, x);
        }
    }

    x_m
}
