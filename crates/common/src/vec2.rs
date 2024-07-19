use serde::{Deserialize, Serialize};
use std::ops::{Index, IndexMut};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vec2<T> {
    shape: (usize, usize),
    base: Vec<T>,
}

impl<T: Default + Copy> Vec2<T> {
    pub fn zeros(shape: (usize, usize)) -> Self {
        Self {
            shape,
            base: vec![T::default(); shape.0 * shape.1],
        }
    }
    pub fn from_vec(shape: (usize, usize), base: Vec<T>) -> Self {
        assert_eq!(shape.0 * shape.1, base.len());
        Self { shape, base }
    }
}

impl<T: Copy> Vec2<T> {
    pub fn copy_within(&mut self, (l_i,): (usize,), (r_i,): (usize,)) {
        assert!(l_i < self.shape.0);
        assert!(r_i < self.shape.0);
        let src_from = l_i * self.shape.1;
        let src_to = src_from + self.shape.1;
        let dest = r_i * self.shape.1;
        self.base.copy_within(src_from..src_to, dest);
    }
}

impl<T> Vec2<T> {
    pub fn shape_0(&self) -> usize {
        self.shape.0
    }
    pub fn shape_1(&self) -> usize {
        self.shape.1
    }
    pub fn as_slice(&self) -> &[T] {
        self.base.as_slice()
    }
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.base.as_mut_slice()
    }
}

impl<T> Index<(usize,)> for Vec2<T> {
    type Output = [T];

    fn index(&self, (i,): (usize,)) -> &Self::Output {
        &self.base[i * self.shape.1..][..self.shape.1]
    }
}

impl<T> IndexMut<(usize,)> for Vec2<T> {
    fn index_mut(&mut self, (i,): (usize,)) -> &mut Self::Output {
        &mut self.base[i * self.shape.1..][..self.shape.1]
    }
}

impl<T> Index<(usize, usize)> for Vec2<T> {
    type Output = T;

    fn index(&self, (i, j): (usize, usize)) -> &Self::Output {
        &self.base[i * self.shape.1..][j]
    }
}

impl<T> IndexMut<(usize, usize)> for Vec2<T> {
    fn index_mut(&mut self, (i, j): (usize, usize)) -> &mut Self::Output {
        &mut self.base[i * self.shape.1..][j]
    }
}
