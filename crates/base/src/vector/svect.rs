use crate::distance::Distance;
use crate::scalar::ScalarLike;
use crate::vector::{VectorBorrowed, VectorOwned};
use serde::{Deserialize, Serialize};
use std::ops::{Bound, RangeBounds};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SVectOwned<S> {
    dims: u32,
    indexes: Vec<u32>,
    values: Vec<S>,
}

impl<S: ScalarLike> SVectOwned<S> {
    #[inline(always)]
    pub fn new(dims: u32, indexes: Vec<u32>, values: Vec<S>) -> Self {
        Self::new_checked(dims, indexes, values).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: Vec<u32>, values: Vec<S>) -> Option<Self> {
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
        if S::reduce_or_of_is_zero(&values) {
            return None;
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
    pub unsafe fn new_unchecked(dims: u32, indexes: Vec<u32>, values: Vec<S>) -> Self {
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
    pub fn values(&self) -> &[S] {
        &self.values
    }
}

impl<S: ScalarLike> VectorOwned for SVectOwned<S> {
    type Borrowed<'a> = SVectBorrowed<'a, S>;

    #[inline(always)]
    fn as_borrowed(&self) -> SVectBorrowed<'_, S> {
        SVectBorrowed {
            dims: self.dims,
            indexes: &self.indexes,
            values: &self.values,
        }
    }

    #[inline(always)]
    fn zero(dims: u32) -> Self {
        Self::new(dims, vec![], vec![])
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SVectBorrowed<'a, S> {
    dims: u32,
    indexes: &'a [u32],
    values: &'a [S],
}

impl<'a, S: ScalarLike> SVectBorrowed<'a, S> {
    #[inline(always)]
    pub fn new(dims: u32, indexes: &'a [u32], values: &'a [S]) -> Self {
        Self::new_checked(dims, indexes, values).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: &'a [u32], values: &'a [S]) -> Option<Self> {
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
            if values[i] == S::zero() {
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
    pub unsafe fn new_unchecked(dims: u32, indexes: &'a [u32], values: &'a [S]) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }

    #[inline(always)]
    pub fn indexes(&self) -> &'a [u32] {
        self.indexes
    }

    #[inline(always)]
    pub fn values(&self) -> &'a [S] {
        self.values
    }

    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.indexes.len() as u32
    }
}

impl<S: ScalarLike> VectorBorrowed for SVectBorrowed<'_, S> {
    type Owned = SVectOwned<S>;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    #[inline(always)]
    fn own(&self) -> SVectOwned<S> {
        SVectOwned {
            dims: self.dims,
            indexes: self.indexes.to_vec(),
            values: self.values.to_vec(),
        }
    }

    #[inline(always)]
    fn norm(&self) -> f32 {
        S::reduce_sum_of_x2(self.values).sqrt()
    }

    #[inline(always)]
    fn operator_dot(self, rhs: Self) -> Distance {
        let xy = S::reduce_sum_of_sparse_xy(self.indexes, self.values, rhs.indexes, rhs.values);
        Distance::from(-xy)
    }

    #[inline(always)]
    fn operator_l2(self, rhs: Self) -> Distance {
        let d2 = S::reduce_sum_of_sparse_d2(self.indexes, self.values, rhs.indexes, rhs.values);
        Distance::from(d2)
    }

    #[inline(always)]
    fn operator_cos(self, rhs: Self) -> Distance {
        let xy = S::reduce_sum_of_sparse_xy(self.indexes, self.values, rhs.indexes, rhs.values);
        let x2 = S::reduce_sum_of_x2(self.values);
        let y2 = S::reduce_sum_of_x2(rhs.values);
        Distance::from(1.0 - xy / (x2 * y2).sqrt())
    }

    #[inline(always)]
    fn operator_hamming(self, _: Self) -> Distance {
        unimplemented!()
    }

    #[inline(always)]
    fn operator_jaccard(self, _: Self) -> Distance {
        unimplemented!()
    }

    #[inline(always)]
    fn function_normalize(&self) -> SVectOwned<S> {
        let l = S::reduce_sum_of_x2(self.values).sqrt();
        let mut indexes = self.indexes.to_vec();
        let mut values = self.values.to_vec();
        let n = indexes.len();
        S::vector_mul_scalar_inplace(&mut values, 1.0 / l);
        let mut j = 0_usize;
        for i in 0..n {
            if values[i] != S::zero() {
                indexes[j] = indexes[i];
                values[j] = values[i];
                j += 1;
            }
        }
        indexes.truncate(j);
        values.truncate(j);
        SVectOwned::new(self.dims, indexes, values)
    }

    fn operator_add(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        let size1 = self.len();
        let size2 = rhs.len();
        let mut pos1 = 0;
        let mut pos2 = 0;
        let mut pos = 0;
        let mut indexes = vec![0; (size1 + size2) as _];
        let mut values = vec![S::zero(); (size1 + size2) as _];
        while pos1 < size1 && pos2 < size2 {
            let lhs_index = self.indexes[pos1 as usize];
            let rhs_index = rhs.indexes[pos2 as usize];
            let lhs_value = self.values[pos1 as usize];
            let rhs_value = rhs.values[pos2 as usize];
            indexes[pos] = lhs_index.min(rhs_index);
            values[pos] = S::scalar_add(
                lhs_value.mask(lhs_index <= rhs_index),
                rhs_value.mask(lhs_index >= rhs_index),
            );
            pos1 += (lhs_index <= rhs_index) as u32;
            pos2 += (lhs_index >= rhs_index) as u32;
            pos += (values[pos] != S::zero()) as usize;
        }
        for i in pos1..size1 {
            indexes[pos] = self.indexes[i as usize];
            values[pos] = self.values[i as usize];
            pos += 1;
        }
        for i in pos2..size2 {
            indexes[pos] = rhs.indexes[i as usize];
            values[pos] = rhs.values[i as usize];
            pos += 1;
        }
        indexes.truncate(pos);
        values.truncate(pos);
        SVectOwned::new(self.dims, indexes, values)
    }

    fn operator_sub(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        let size1 = self.len();
        let size2 = rhs.len();
        let mut pos1 = 0;
        let mut pos2 = 0;
        let mut pos = 0;
        let mut indexes = vec![0; (size1 + size2) as _];
        let mut values = vec![S::zero(); (size1 + size2) as _];
        while pos1 < size1 && pos2 < size2 {
            let lhs_index = self.indexes[pos1 as usize];
            let rhs_index = rhs.indexes[pos2 as usize];
            let lhs_value = self.values[pos1 as usize];
            let rhs_value = rhs.values[pos2 as usize];
            indexes[pos] = lhs_index.min(rhs_index);
            values[pos] = S::scalar_sub(
                lhs_value.mask(lhs_index <= rhs_index),
                rhs_value.mask(lhs_index >= rhs_index),
            );
            pos1 += (lhs_index <= rhs_index) as u32;
            pos2 += (lhs_index >= rhs_index) as u32;
            pos += (values[pos] != S::zero()) as usize;
        }
        for i in pos1..size1 {
            indexes[pos] = self.indexes[i as usize];
            values[pos] = self.values[i as usize];
            pos += 1;
        }
        for i in pos2..size2 {
            indexes[pos] = rhs.indexes[i as usize];
            values[pos] = S::scalar_neg(rhs.values[i as usize]);
            pos += 1;
        }
        indexes.truncate(pos);
        values.truncate(pos);
        SVectOwned::new(self.dims, indexes, values)
    }

    fn operator_mul(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        let size1 = self.len();
        let size2 = rhs.len();
        let mut pos1 = 0;
        let mut pos2 = 0;
        let mut pos = 0;
        let mut indexes = vec![0; std::cmp::min(size1, size2) as _];
        let mut values = vec![S::zero(); std::cmp::min(size1, size2) as _];
        while pos1 < size1 && pos2 < size2 {
            let lhs_index = self.indexes[pos1 as usize];
            let rhs_index = rhs.indexes[pos2 as usize];
            match lhs_index.cmp(&rhs_index) {
                std::cmp::Ordering::Less => {
                    pos1 += 1;
                }
                std::cmp::Ordering::Equal => {
                    // only both indexes are not zero, values are multiplied
                    let lhs_value = self.values[pos1 as usize];
                    let rhs_value = rhs.values[pos2 as usize];
                    indexes[pos] = lhs_index;
                    values[pos] = S::scalar_mul(lhs_value, rhs_value);
                    pos1 += 1;
                    pos2 += 1;
                    // only increment pos if the value is not zero
                    pos += (values[pos] != S::zero()) as usize;
                }
                std::cmp::Ordering::Greater => {
                    pos2 += 1;
                }
            }
        }
        indexes.truncate(pos);
        values.truncate(pos);
        SVectOwned::new(self.dims, indexes, values)
    }

    fn operator_and(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_or(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_xor(&self, _: Self) -> Self::Owned {
        unimplemented!()
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
        let s = self.indexes.partition_point(|&x| x < start);
        let e = self.indexes.partition_point(|&x| x < end);
        let indexes = self.indexes[s..e]
            .iter()
            .map(|x| x - start)
            .collect::<Vec<_>>();
        let values = self.values[s..e].to_vec();
        Self::Owned::new_checked(dims, indexes, values)
    }
}

impl<S: ScalarLike> PartialEq for SVectBorrowed<'_, S> {
    fn eq(&self, other: &Self) -> bool {
        if self.dims != other.dims {
            return false;
        }
        if self.indexes.len() != other.indexes.len() {
            return false;
        }
        for (&l, &r) in self.indexes.iter().zip(other.indexes.iter()) {
            if l != r {
                return false;
            }
        }
        for (&l, &r) in self.values.iter().zip(other.values.iter()) {
            if l != r {
                return false;
            }
        }
        true
    }
}

impl<S: ScalarLike> PartialOrd for SVectBorrowed<'_, S> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        if self.dims != other.dims {
            return None;
        }
        let mut lhs = self
            .indexes
            .iter()
            .copied()
            .zip(self.values.iter().copied());
        let mut rhs = other
            .indexes
            .iter()
            .copied()
            .zip(other.values.iter().copied());
        loop {
            return match (lhs.next(), rhs.next()) {
                (Some(lh), Some(rh)) => match lh.0.cmp(&rh.0) {
                    Ordering::Equal => match lh.1.partial_cmp(&rh.1)? {
                        Ordering::Equal => continue,
                        x => Some(x),
                    },
                    Ordering::Less => Some(if lh.1 < S::zero() {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }),
                    Ordering::Greater => Some(if S::zero() < rh.1 {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }),
                },
                (Some((_, x)), None) => Some(PartialOrd::partial_cmp(&x, &S::zero())?),
                (None, Some((_, y))) => Some(PartialOrd::partial_cmp(&S::zero(), &y)?),
                (None, None) => Some(Ordering::Equal),
            };
        }
    }
}
