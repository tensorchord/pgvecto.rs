use std::ops::{Bound, RangeBounds};

use crate::distance::Distance;
use crate::scalar::bit;
use crate::vector::{VectorBorrowed, VectorOwned};
use serde::{Deserialize, Serialize};

pub const BVECTOR_WIDTH: u32 = u64::BITS;

// When using binary vector, please ensure that the padding bits are always zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BVectOwned {
    dims: u32,
    data: Vec<u64>,
}

impl BVectOwned {
    #[inline(always)]
    pub fn new(dims: u32, data: Vec<u64>) -> Self {
        Self::new_checked(dims, data).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, data: Vec<u64>) -> Option<Self> {
        if !(1..=65535).contains(&dims) {
            return None;
        }
        if data.len() != dims.div_ceil(BVECTOR_WIDTH) as usize {
            return None;
        }
        if dims % BVECTOR_WIDTH != 0 && data[data.len() - 1] >> (dims % BVECTOR_WIDTH) != 0 {
            return None;
        }
        unsafe { Some(Self::new_unchecked(dims, data)) }
    }

    /// # Safety
    ///
    /// * `dims` must be in `1..=65535`.
    /// * `data` must be of the correct length.
    /// * The padding bits must be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, data: Vec<u64>) -> Self {
        Self { dims, data }
    }
}

impl VectorOwned for BVectOwned {
    type Borrowed<'a> = BVectBorrowed<'a>;

    #[inline(always)]
    fn as_borrowed(&self) -> BVectBorrowed<'_> {
        BVectBorrowed {
            dims: self.dims,
            data: &self.data,
        }
    }

    #[inline(always)]
    fn zero(dims: u32) -> Self {
        Self::new(dims, vec![0; dims.div_ceil(BVECTOR_WIDTH) as usize])
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BVectBorrowed<'a> {
    dims: u32,
    data: &'a [u64],
}

impl<'a> BVectBorrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u32, data: &'a [u64]) -> Self {
        Self::new_checked(dims, data).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, data: &'a [u64]) -> Option<Self> {
        if !(1..=65535).contains(&dims) {
            return None;
        }
        if data.len() != dims.div_ceil(BVECTOR_WIDTH) as usize {
            return None;
        }
        if dims % BVECTOR_WIDTH != 0 && data[data.len() - 1] >> (dims % BVECTOR_WIDTH) != 0 {
            return None;
        }
        unsafe { Some(Self::new_unchecked(dims, data)) }
    }

    /// # Safety
    ///
    /// * `dims` must be in `1..=65535`.
    /// * `data` must be of the correct length.
    /// * The padding bits must be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, data: &'a [u64]) -> Self {
        Self { dims, data }
    }

    #[inline(always)]
    pub fn data(&self) -> &'a [u64] {
        self.data
    }

    #[inline(always)]
    pub fn get(&self, index: u32) -> bool {
        assert!(index < self.dims);
        self.data[(index / BVECTOR_WIDTH) as usize] & (1 << (index % BVECTOR_WIDTH)) != 0
    }

    #[inline(always)]
    pub fn iter(self) -> impl Iterator<Item = bool> + 'a {
        let mut index = 0_u32;
        std::iter::from_fn(move || {
            if index < self.dims {
                let result = self.data[(index / BVECTOR_WIDTH) as usize]
                    & (1 << (index % BVECTOR_WIDTH))
                    != 0;
                index += 1;
                Some(result)
            } else {
                None
            }
        })
    }
}

impl VectorBorrowed for BVectBorrowed<'_> {
    type Owned = BVectOwned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn own(&self) -> BVectOwned {
        BVectOwned {
            dims: self.dims,
            data: self.data.to_vec(),
        }
    }

    #[inline(always)]
    fn norm(&self) -> f32 {
        (bit::sum_of_x(self.data) as f32).sqrt()
    }

    #[inline(always)]
    fn operator_dot(self, rhs: Self) -> Distance {
        Distance::from(-(bit::sum_of_and(self.data, rhs.data) as f32))
    }

    #[inline(always)]
    fn operator_l2(self, _: Self) -> Distance {
        unimplemented!()
    }

    #[inline(always)]
    fn operator_cos(self, _: Self) -> Distance {
        unimplemented!()
    }

    #[inline(always)]
    fn operator_hamming(self, rhs: Self) -> Distance {
        Distance::from(bit::sum_of_xor(self.data, rhs.data) as f32)
    }

    #[inline(always)]
    fn operator_jaccard(self, rhs: Self) -> Distance {
        let (and, or) = bit::sum_of_and_or(self.data, rhs.data);
        Distance::from(1.0 - (and as f32 / or as f32))
    }

    #[inline(always)]
    fn function_normalize(&self) -> BVectOwned {
        unimplemented!()
    }

    fn operator_add(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_sub(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_mul(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_and(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        let data = bit::vector_and(self.data, self.data);
        BVectOwned::new(self.dims, data)
    }

    fn operator_or(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        let data = bit::vector_or(self.data, rhs.data);
        BVectOwned::new(self.dims, data)
    }

    fn operator_xor(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        let data = bit::vector_xor(self.data, rhs.data);
        BVectOwned::new(self.dims, data)
    }

    #[inline(always)]
    fn subvector(&self, bounds: impl RangeBounds<u32>) -> Option<Self::Owned> {
        let start = match bounds.start_bound().cloned() {
            Bound::Included(x) => x,
            Bound::Excluded(u32::MAX) => return None,
            Bound::Excluded(x) => x + 1,
            Bound::Unbounded => 0,
        };
        let end = match bounds.end_bound().cloned() {
            Bound::Included(u32::MAX) => return None,
            Bound::Included(x) => x + 1,
            Bound::Excluded(x) => x,
            Bound::Unbounded => self.dims,
        };
        if start >= end || end > self.dims {
            return None;
        }
        let dims = end - start;
        let mut data = vec![0_u64; dims.div_ceil(BVECTOR_WIDTH) as _];
        {
            let mut i = 0;
            let mut j = start;
            while j < end {
                if self.data[(j / BVECTOR_WIDTH) as usize] & (1 << (j % BVECTOR_WIDTH)) != 0 {
                    data[(i / BVECTOR_WIDTH) as usize] |= 1 << (i % BVECTOR_WIDTH);
                }
                i += 1;
                j += 1;
            }
        }
        Self::Owned::new_checked(dims, data)
    }
}

impl PartialEq for BVectBorrowed<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.dims != other.dims {
            return false;
        }
        for (&l, &r) in self.data.iter().zip(other.data.iter()) {
            let l = l.reverse_bits();
            let r = r.reverse_bits();
            if l != r {
                return false;
            }
        }
        true
    }
}

impl PartialOrd for BVectBorrowed<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        if self.dims != other.dims {
            return None;
        }
        for (&l, &r) in self.data.iter().zip(other.data.iter()) {
            let l = l.reverse_bits();
            let r = r.reverse_bits();
            match l.cmp(&r) {
                Ordering::Equal => (),
                x => return Some(x),
            }
        }
        Some(Ordering::Equal)
    }
}
