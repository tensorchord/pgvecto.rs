use base::search::Vectors;
use base::vector::{VectBorrowed, VectOwned};
use ndarray::{s, ArrayView2};

pub struct Dataset<'a> {
    underlying: ArrayView2<'a, f32>,
}

impl<'a> Dataset<'a> {
    pub fn new(dataset: ArrayView2<'a, f32>) -> Self {
        assert!(1 <= dataset.dim().1 && dataset.dim().1 <= 65535);
        assert!(dataset.dim().1 <= u32::MAX as usize);
        assert!(dataset.dim().0 <= u32::MAX as usize);
        Self {
            underlying: dataset,
        }
    }
}

impl<'a> Vectors<VectOwned<f32>> for Dataset<'a> {
    fn dims(&self) -> u32 {
        self.underlying.dim().1 as _
    }

    fn len(&self) -> u32 {
        self.underlying.dim().0 as _
    }

    fn vector(&self, i: u32) -> VectBorrowed<'_, f32> {
        let s = self
            .underlying
            .slice(s!(i as usize, ..))
            .to_slice()
            .expect("memory is non continuous");
        fn cast(x: &[f32]) -> &[f32] {
            unsafe { std::mem::transmute(x) }
        }
        VectBorrowed::new(cast(s))
    }
}
