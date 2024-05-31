#![allow(unused_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]

use super::get_mut_internal;
use super::memory_svecf32::*;
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
use pgrx::Internal;

pub struct SVecf32AggregateAvgSumStype {
    dims: u32,
    len: u32,
    capacity: u32,
    count: u64,
    indexes: Vec<u32>,
    values: Vec<F32>,
}

impl SVecf32AggregateAvgSumStype {
    pub fn dims(&self) -> usize {
        self.dims as usize
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    /// Get the indexes of the sparse state.
    fn indexes(&self) -> &[u32] {
        &self.indexes.as_slice()[0..self.len()]
    }
    /// Get the values of the sparse state.
    fn values(&self) -> &[F32] {
        &self.values.as_slice()[0..self.len()]
    }
    /// Get the mutable references of the indexes and values of the sparse state. The indexes and values may contain reserved elements.
    fn indexes_values_mut(&mut self) -> (&mut [u32], &mut [F32]) {
        (self.indexes.as_mut_slice(), self.values.as_mut_slice())
    }
    /// Filter zero values from the sparse state.
    fn filter_zero(&mut self) {
        let len = self.len();
        let mut i = 0;
        let mut j = 0;
        let (indexes, values) = self.indexes_values_mut();
        while i < len {
            if !values[i].is_zero() {
                indexes[j] = indexes[i];
                values[j] = values[i];
                j += 1;
            }
            i += 1;
        }
        self.len = j as u32;
    }

    /// Create a new sparse accumulate state with a given capacity.
    pub fn new_with_capacity(dims: u32, capacity: usize) -> Self {
        // set capacity at least 16
        let capacity = std::cmp::max(usize::next_power_of_two(capacity), 16);
        // set capacity at most dims
        let capacity = std::cmp::min(capacity, dims as usize);
        let indexes = vec![0; capacity];
        let values = vec![F32::zero(); capacity];
        Self {
            dims,
            len: 0,
            capacity: capacity as u32,
            count: 0,
            indexes,
            values,
        }
    }

    pub fn for_mut_borrow(&mut self) -> SVecf32AggregateAvgSumStypeBorrowed<'_> {
        let dims = self.dims() as u32;
        let len = self.len() as u32;
        let capacity = self.capacity() as u32;
        let count = self.count();
        let (indexes, values) = self.indexes_values_mut();
        SVecf32AggregateAvgSumStypeBorrowed::new(dims, len, capacity, count, indexes, values)
    }

    /// check whether the rest of the state is enough to append the sparse vector of the given length. Approximately predict the rest of the state is enough.
    pub fn check_capacity(&self, length: usize) -> bool {
        // If the state is full, return true. So we can enlarger the state less than log2(n) times.
        if self.capacity() == self.dims() {
            return true;
        }
        let capacity = self.capacity();
        let rest = capacity - self.len();
        rest >= length
    }

    pub fn merge_in_place(&mut self, svec: SVecf32Borrowed<'_>) {
        let len = self.for_mut_borrow().merge_in_place(svec);
        self.len = len;
    }
}

pub struct SVecf32AggregateAvgSumStypeBorrowed<'a> {
    #[allow(dead_code)]
    dims: u32,
    len: u32,
    capacity: u32,
    #[allow(dead_code)]
    count: u64,
    indexes: &'a mut [u32],
    values: &'a mut [F32],
}

impl<'a> SVecf32AggregateAvgSumStypeBorrowed<'a> {
    pub fn new(
        dims: u32,
        len: u32,
        capacity: u32,
        count: u64,
        indexes: &'a mut [u32],
        values: &'a mut [F32],
    ) -> Self {
        Self {
            dims,
            len,
            capacity,
            count,
            indexes,
            values,
        }
    }

    /// Merge a sparse vector into the state in place.
    /// To promise the state is enough to merge the sparse vector, the caller should check the capacity of the state before calling this function.
    pub fn merge_in_place(&mut self, svec: SVecf32Borrowed<'_>) -> u32 {
        let sindexes = svec.indexes();
        let svalues = svec.values();
        let slen = sindexes.len();
        let len = self.len;
        let capacity = self.capacity as usize;
        // To reduce the memory usage, we try to merge the sparse vector in place.
        // If the state capacity is enough, we can merge the sparse vector from the end. Then copy the result to the beginning. The merge process compares the indexes of the sparse vector and the state from end to start, and the result is stored in the state from the end to start to avoid the conflict.
        let mut i = (len as i32) - 1;
        let mut j = (slen as i32) - 1;
        let mut p = (capacity as i32) - 1;
        while i >= 0 && j >= 0 {
            let index = self.indexes[i as usize];
            let sindex = sindexes[j as usize];
            let value = self.values[i as usize];
            let svalue = svalues[j as usize];
            let pi = std::cmp::max(index, sindex);
            let pv = F32((pi == index) as usize as f32) * value
                + F32((pi == sindex) as usize as f32) * svalue;
            i -= (index >= sindex) as i32;
            j -= (index <= sindex) as i32;
            assert!(p > i, "Conflict occurs when merge in place.");
            self.indexes[p as usize] = pi;
            self.values[p as usize] = pv;
            // Skip zero value.
            p -= (!pv.is_zero()) as i32;
        }
        while j >= 0 {
            assert!(p > i, "Conflict occurs when merge in place.");
            self.indexes[p as usize] = sindexes[j as usize];
            self.values[p as usize] = svalues[j as usize];
            p -= 1;
            j -= 1;
        }
        self.len = if i < 0 {
            // move the whole state [p+1..capacity] to the beginning
            self.indexes.copy_within(((p + 1) as usize)..capacity, 0);
            self.values.copy_within(((p + 1) as usize)..capacity, 0);
            ((capacity as i32) - p - 1) as u32
        } else {
            // concatenate the state [p+1..capacity] and remaining state [0..i]
            self.indexes
                .copy_within(((p + 1) as usize)..capacity, (i as usize) + 1);
            self.values
                .copy_within(((p + 1) as usize)..capacity, (i as usize) + 1);
            ((capacity as i32) - p + i) as u32
        };
        self.len
    }
}

/// accumulate intermediate state for sparse vector
#[base_macros::aggregate_func]
#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_sum_sfunc(
    mut current: Option<Internal>,
    value: Option<SVecf32Input<'_>>,
) -> Option<Internal> {
    if value.is_none() {
        // It would get error "returned Datum was NULL" if we use Internal directly when execute `SELECT sum(v) FROM unnest(ARRAY[NULL]::svector[]) v;`
        // so we return a optional here. But it is so reasonable though.
        match get_mut_internal::<SVecf32AggregateAvgSumStype>(&mut current) {
            Some(_) => {
                return current;
            }
            None => {
                return None;
            }
        }
    }
    let value = value.unwrap();
    match get_mut_internal::<SVecf32AggregateAvgSumStype>(&mut current) {
        // if the state is empty, copy the input vector
        None => {
            let internal = Internal::new(SVecf32AggregateAvgSumStype::new_with_capacity(
                value.dims() as u32,
                value.len(),
            ));
            let state = unsafe { internal.get_mut::<SVecf32AggregateAvgSumStype>().unwrap() };
            state.merge_in_place(value.for_borrow());
            state.count = 1;
            Some(internal)
        }
        Some(state) => {
            let dims = state.dims();
            let value_dims = value.dims();
            check_matched_dims(dims, value_dims);
            let count = state.count() + 1;
            match state.check_capacity(value.len()) {
                true => {
                    // merge the input vector into state
                    state.merge_in_place(value.for_borrow());
                    state.count = count;
                    // return old state
                    current
                }
                false => {
                    // allocate a new state and merge the old state
                    let new_internal =
                        Internal::new(SVecf32AggregateAvgSumStype::new_with_capacity(
                            dims as u32,
                            state.len() + value.len(),
                        ));
                    let new_state = unsafe {
                        new_internal
                            .get_mut::<SVecf32AggregateAvgSumStype>()
                            .unwrap()
                    };
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        dims as u32,
                        state.indexes(),
                        state.values(),
                    ));
                    // merge the input vector into state
                    new_state.merge_in_place(value.for_borrow());
                    new_state.count = count;
                    Some(new_internal)
                }
            }
        }
    }
}

/// combine two intermediate states for sparse vector
#[base_macros::aggregate_func]
#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_sum_combinefunc(
    mut state1: Option<Internal>,
    mut state2: Option<Internal>,
) -> Option<Internal> {
    match (
        get_mut_internal::<SVecf32AggregateAvgSumStype>(&mut state1),
        get_mut_internal::<SVecf32AggregateAvgSumStype>(&mut state2),
    ) {
        (None, None) => state1,
        (Some(_), None) => state1,
        (None, Some(_)) => state2,
        (Some(s1), Some(s2)) => {
            let dims1 = s1.dims();
            let dims2 = s2.dims();
            check_matched_dims(dims1, dims2);
            // ensure state1 has larger capacity
            let (s1, s2, larger_internal) = if s1.capacity() > s2.capacity() {
                (s1, s2, 0)
            } else {
                (s2, s1, 1)
            };
            let total_count = s1.count() + s2.count();
            match s1.check_capacity(s2.len()) {
                true => {
                    // merge state2 into state
                    s1.merge_in_place(SVecf32Borrowed::new(
                        s2.dims() as u32,
                        s2.indexes(),
                        s2.values(),
                    ));
                    s1.count = total_count;
                    if larger_internal == 0 {
                        state1
                    } else {
                        state2
                    }
                }
                false => {
                    // allocate a new state and merge the old state
                    let mut new_state = SVecf32AggregateAvgSumStype::new_with_capacity(
                        dims1 as u32,
                        s1.len() + s2.len(),
                    );
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        dims1 as u32,
                        s1.indexes(),
                        s1.values(),
                    ));
                    // merge state2 into state
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        s2.dims() as u32,
                        s2.indexes(),
                        s2.values(),
                    ));
                    new_state.count = total_count;
                    Some(Internal::new(new_state))
                }
            }
        }
    }
}

/// finalize the intermediate state for sparse vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_finalfunc(mut state: Option<Internal>) -> Option<SVecf32Output> {
    match get_mut_internal::<SVecf32AggregateAvgSumStype>(&mut state) {
        Some(state) => {
            let len = state.len();
            let count = state.count();
            state.indexes_values_mut().1[..len]
                .iter_mut()
                .for_each(|x| *x /= count as f32);
            state.filter_zero();
            let indexes = state.indexes();
            let values = state.values();
            Some(SVecf32Output::new(SVecf32Borrowed::new(
                state.dims() as u32,
                indexes,
                values,
            )))
        }
        None => None,
    }
}

/// finalize the intermediate state for sparse vector sum
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_sum_finalfunc(mut state: Option<Internal>) -> Option<SVecf32Output> {
    match get_mut_internal::<SVecf32AggregateAvgSumStype>(&mut state) {
        Some(state) => {
            state.filter_zero();
            let indexes = state.indexes();
            let values = state.values();
            Some(SVecf32Output::new(SVecf32Borrowed::new(
                state.dims() as u32,
                indexes,
                values,
            )))
        }
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_in_place() {
        // test merge_in_place success
        let indexes_20: Vec<u32> = vec![
            1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let values_20: Vec<F32> = vec![
            1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        .iter()
        .map(|&x| F32(x as f32))
        .collect();
        let mut indexes = indexes_20.clone();
        let mut values = values_20.clone();
        let dims = 20;
        let mut len = 10;
        let mut capacity = 20;
        let count = 1;
        let mut state = SVecf32AggregateAvgSumStypeBorrowed::new(
            dims,
            len,
            capacity,
            count,
            indexes.as_mut_slice(),
            values.as_mut_slice(),
        );
        let sindexes = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18];
        let svalues: Vec<F32> = vec![1, 2, 4, 6, 8, 10, 12, 14, 16, 18]
            .iter()
            .map(|&x| F32(x as f32))
            .collect();
        let svec = SVecf32Borrowed::new(dims, sindexes.as_slice(), svalues.as_slice());
        state.merge_in_place(svec);
        assert_eq!(state.len, 20);
        assert_eq!(
            indexes,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19].as_slice()
        );
        assert_eq!(
            values,
            vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
                .iter()
                .map(|&x| F32(x as f32))
                .collect::<Vec<F32>>()
                .as_slice()
        );
        // test merge_in_place result is full
        len = 6;
        capacity = 16;
        let mut indexes = indexes_20.clone();
        let mut values = values_20.clone();
        let mut state = SVecf32AggregateAvgSumStypeBorrowed::new(
            dims,
            len,
            capacity,
            count,
            indexes.as_mut_slice(),
            values.as_mut_slice(),
        );
        state.merge_in_place(svec);
        let result_len = state.len;
        assert_eq!(result_len, 16);
        assert_eq!(
            indexes[0..(result_len as usize)],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18]
                .iter()
                .map(|&x| x as u32)
                .collect::<Vec<u32>>()
        );
        assert_eq!(
            values[0..(result_len as usize)],
            vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18]
                .iter()
                .map(|&x| F32(x as f32))
                .collect::<Vec<F32>>()
        );
        // test indexes overlap
        let mut indexes = indexes_20.clone();
        let mut values = values_20.clone();
        len = 10;
        capacity = 20;
        let mut state = SVecf32AggregateAvgSumStypeBorrowed::new(
            dims,
            len,
            capacity,
            count,
            indexes.as_mut_slice(),
            values.as_mut_slice(),
        );
        let sindexes = vec![0, 3, 6, 9, 12, 15, 18];
        let svalues: Vec<F32> = vec![1, 1, 1, 1, 1, 1, 1]
            .iter()
            .map(|&x| F32(x as f32))
            .collect();
        let svec = SVecf32Borrowed::new(dims, sindexes.as_slice(), svalues.as_slice());
        state.merge_in_place(svec);
        let result_len = state.len;
        assert_eq!(result_len, 14);
        assert_eq!(
            indexes[0..(result_len as usize)],
            vec![0, 1, 3, 5, 6, 7, 9, 11, 12, 13, 15, 17, 18, 19]
                .iter()
                .map(|&x| x as u32)
                .collect::<Vec<u32>>()
        );
        assert_eq!(
            values[0..(result_len as usize)],
            vec![1, 1, 4, 5, 1, 7, 10, 11, 1, 13, 16, 17, 1, 19]
                .iter()
                .map(|&x| F32(x as f32))
                .collect::<Vec<F32>>()
        );
    }
}
