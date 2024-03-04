use super::{VectorBorrowed, VectorOwned};
use crate::scalar::F32;
use serde::{Deserialize, Serialize};

pub const BVEC_WIDTH: usize = usize::BITS as usize;

// When using binary vector, please ensure that the padding bits are always zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BVecf32Owned {
    dims: u16,
    data: Vec<usize>,
}

impl BVecf32Owned {
    pub fn new_zeroed(dims: u16) -> Self {
        assert!((1..=65535).contains(&dims));
        let size = (dims as usize).div_ceil(BVEC_WIDTH);
        Self {
            dims,
            data: vec![0; size],
        }
    }

    pub fn set(&mut self, index: usize, value: bool) {
        assert!(index < self.dims as usize);
        if value {
            self.data[index / BVEC_WIDTH] |= 1 << (index % BVEC_WIDTH);
        } else {
            self.data[index / BVEC_WIDTH] &= !(1 << (index % BVEC_WIDTH));
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that it won't modify the padding bits
    #[inline(always)]
    pub unsafe fn data_mut(&mut self) -> &mut [usize] {
        &mut self.data
    }
}

impl VectorOwned for BVecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = BVecf32Borrowed<'a>;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn for_borrow(&self) -> BVecf32Borrowed<'_> {
        BVecf32Borrowed {
            dims: self.dims,
            data: &self.data,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        self.for_borrow().to_vec()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BVecf32Borrowed<'a> {
    dims: u16,
    data: &'a [usize],
}

impl<'a> BVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u16, data: &'a [usize]) -> Self {
        Self::new_checked(dims, data).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u16, data: &'a [usize]) -> Option<Self> {
        if dims == 0 {
            return None;
        }
        if data.len() != (dims as usize).div_ceil(BVEC_WIDTH) {
            return None;
        }
        if data[data.len() - 1] >> (dims % BVEC_WIDTH as u16) != 0 {
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
    pub unsafe fn new_unchecked(dims: u16, data: &'a [usize]) -> Self {
        Self { dims, data }
    }

    pub fn iter(self) -> impl Iterator<Item = bool> + 'a {
        let mut index = 0;
        std::iter::from_fn(move || {
            if index < self.dims as usize {
                let result = self.data[index / BVEC_WIDTH] & (1 << (index % BVEC_WIDTH)) != 0;
                index += 1;
                Some(result)
            } else {
                None
            }
        })
    }

    pub fn get(&self, index: usize) -> bool {
        assert!(index < self.dims as usize);
        self.data[index / BVEC_WIDTH] & (1 << (index % BVEC_WIDTH)) != 0
    }

    #[inline(always)]
    pub fn data(&self) -> &'a [usize] {
        self.data
    }
}

impl<'a> VectorBorrowed for BVecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = BVecf32Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn for_own(&self) -> BVecf32Owned {
        BVecf32Owned {
            dims: self.dims,
            data: self.data.to_vec(),
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        self.iter().map(|i| F32(i as u32 as f32)).collect()
    }
}

impl<'a> Ord for BVecf32Borrowed<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        assert_eq!(self.dims, other.dims);
        for (&l, &r) in self.data.iter().zip(other.data.iter()) {
            let l = l.reverse_bits();
            let r = r.reverse_bits();
            match l.cmp(&r) {
                std::cmp::Ordering::Equal => {}
                x => return x,
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl<'a> PartialOrd for BVecf32Borrowed<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
