use crate::scalar::F32;
use crate::vector::{VectorBorrowed, VectorKind, VectorOwned};
use num_traits::{Float, Zero};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SVecf32Owned {
    dims: u32,
    indexes: Vec<u32>,
    values: Vec<F32>,
}

impl SVecf32Owned {
    #[inline(always)]
    pub fn new(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Self {
        Self::new_checked(dims, indexes, values).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Option<Self> {
        if !(1..=1_048_575).contains(&dims) {
            return None;
        }
        if indexes.len() != values.len() {
            return None;
        }
        let len = indexes.len();
        for i in 1..len {
            if !(indexes[i - 1] < indexes[i]) {
                return None;
            }
        }
        if len != 0 && !(indexes[len - 1] < dims) {
            return None;
        }
        for i in 0..len {
            if values[i].is_zero() {
                return None;
            }
        }
        unsafe { Some(Self::new_unchecked(dims, indexes, values)) }
    }
    /// # Safety
    ///
    /// * `dims` must be in `1..=1_048_575`.
    /// * `indexes.len()` must be equal to `values.len()`.
    /// * `indexes` must be a strictly increasing sequence and the last in the sequence must be less than `dims`.
    /// * A floating number in `values` must not be positive zero or negative zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }
    #[inline(always)]
    pub fn indexes(&self) -> &[u32] {
        &self.indexes
    }
    #[inline(always)]
    pub fn values(&self) -> &[F32] {
        &self.values
    }
}

impl VectorOwned for SVecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = SVecf32Borrowed<'a>;

    const VECTOR_KIND: VectorKind = VectorKind::SVecf32;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_borrow(&self) -> SVecf32Borrowed<'_> {
        SVecf32Borrowed {
            dims: self.dims,
            indexes: &self.indexes,
            values: &self.values,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SVecf32Borrowed<'a> {
    dims: u32,
    indexes: &'a [u32],
    values: &'a [F32],
}

impl<'a> SVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Self {
        Self::new_checked(dims, indexes, values).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Option<Self> {
        if !(1..=1_048_575).contains(&dims) {
            return None;
        }
        if indexes.len() != values.len() {
            return None;
        }
        let len = indexes.len();
        for i in 1..len {
            if !(indexes[i - 1] < indexes[i]) {
                return None;
            }
        }
        if len != 0 && !(indexes[len - 1] < dims) {
            return None;
        }
        for i in 0..len {
            if values[i].is_zero() {
                return None;
            }
        }
        unsafe { Some(Self::new_unchecked(dims, indexes, values)) }
    }
    /// # Safety
    ///
    /// * `dims` must be in `1..=1_048_575`.
    /// * `indexes.len()` must be equal to `values.len()`.
    /// * `indexes` must be a strictly increasing sequence and the last in the sequence must be less than `dims`.
    /// * A floating number in `values` must not be positive zero or negative zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }
    #[inline(always)]
    pub fn indexes(&self) -> &[u32] {
        self.indexes
    }
    #[inline(always)]
    pub fn values(&self) -> &[F32] {
        self.values
    }
}

impl<'a> VectorBorrowed for SVecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = SVecf32Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_own(&self) -> SVecf32Owned {
        SVecf32Owned {
            dims: self.dims,
            indexes: self.indexes.to_vec(),
            values: self.values.to_vec(),
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }
}

impl<'a> SVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.indexes.len().try_into().unwrap()
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        let lhs_value = lhs.values()[lhs_pos];
        let rhs_value = rhs.values()[rhs_pos];
        xy += F32((lhs_index == rhs_index) as u32 as f32) * lhs_value * rhs_value;
        x2 += F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value * lhs_value;
        y2 += F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value * rhs_value;
        lhs_pos += (lhs_index <= rhs_index) as usize;
        rhs_pos += (lhs_index >= rhs_index) as usize;
    }
    for i in lhs_pos..size1 {
        x2 += lhs.values()[i] * lhs.values()[i];
    }
    for i in rhs_pos..size2 {
        y2 += rhs.values()[i] * rhs.values()[i];
    }
    xy / (x2 * y2).sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut xy = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        let lhs_value = lhs.values()[lhs_pos];
        let rhs_value = rhs.values()[rhs_pos];
        xy += F32((lhs_index == rhs_index) as u32 as f32) * lhs_value * rhs_value;
        lhs_pos += (lhs_index <= rhs_index) as usize;
        rhs_pos += (lhs_index >= rhs_index) as usize;
    }
    xy
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot_2<'a>(lhs: SVecf32Borrowed<'a>, rhs: &[F32]) -> F32 {
    let mut xy = F32::zero();
    for i in 0..lhs.len() as usize {
        xy += lhs.values()[i] * rhs[lhs.indexes()[i] as usize];
    }
    xy
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn sl2<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut d2 = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        let lhs_value = lhs.values()[lhs_pos];
        let rhs_value = rhs.values()[rhs_pos];
        let d = F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value
            - F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value;
        d2 += d * d;
        lhs_pos += (lhs_index <= rhs_index) as usize;
        rhs_pos += (lhs_index >= rhs_index) as usize;
    }
    for i in lhs_pos..size1 {
        d2 += lhs.values()[i] * lhs.values()[i];
    }
    for i in rhs_pos..size2 {
        d2 += rhs.values()[i] * rhs.values()[i];
    }
    d2
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn sl2_2<'a>(lhs: SVecf32Borrowed<'a>, rhs: &[F32]) -> F32 {
    let mut d2 = F32::zero();
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    while lhs_pos < lhs.len() {
        let index_eq = lhs.indexes()[lhs_pos as usize] == rhs_pos;
        let d =
            F32(index_eq as u32 as f32) * lhs.values()[lhs_pos as usize] - rhs[rhs_pos as usize];
        d2 += d * d;
        lhs_pos += index_eq as u32;
        rhs_pos += 1;
    }
    for i in rhs_pos..rhs.len() as u32 {
        d2 += rhs[i as usize] * rhs[i as usize];
    }
    d2
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn length<'a>(vector: SVecf32Borrowed<'a>) -> F32 {
    let mut dot = F32::zero();
    for &i in vector.values() {
        dot += i * i;
    }
    dot.sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn l2_normalize(vector: &mut SVecf32Owned) {
    let l = length(vector.for_borrow());
    let dims = vector.dims();
    let indexes = vector.indexes().to_vec();
    let mut values = vector.values().to_vec();
    for i in values.iter_mut() {
        *i /= l;
    }
    *vector = SVecf32Owned::new(dims, indexes, values);
}
