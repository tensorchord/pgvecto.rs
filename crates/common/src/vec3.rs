use base::pod::Pod;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut, Index, IndexMut};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vec3<T> {
    x: usize,
    y: usize,
    z: usize,
    v: Vec<T>,
}

impl<T: Pod + Ord> Vec3<T> {
    pub fn new(x: usize, y: usize, z: usize) -> Self {
        Self {
            x,
            y,
            z,
            v: base::pod::zeroed_vec(x * y * z),
        }
    }
    pub fn x(&self) -> usize {
        self.x
    }
    pub fn y(&self) -> usize {
        self.y
    }
    pub fn z(&self) -> usize {
        self.z
    }
}

impl<T> Index<()> for Vec3<T> {
    type Output = [T];

    fn index(&self, (): ()) -> &Self::Output {
        &self.v[..]
    }
}

impl<T> IndexMut<()> for Vec3<T> {
    fn index_mut(&mut self, (): ()) -> &mut Self::Output {
        &mut self.v[..]
    }
}

impl<T> Index<(usize,)> for Vec3<T> {
    type Output = [T];

    fn index(&self, (x,): (usize,)) -> &Self::Output {
        &self.v[x * self.y * self.z..][..self.y * self.z]
    }
}

impl<T> IndexMut<(usize,)> for Vec3<T> {
    fn index_mut(&mut self, (x,): (usize,)) -> &mut Self::Output {
        &mut self.v[x * self.y * self.z..][..self.y * self.z]
    }
}

impl<T> Index<(usize, usize)> for Vec3<T> {
    type Output = [T];

    fn index(&self, (x, y): (usize, usize)) -> &Self::Output {
        &self.v[x * self.y * self.z + y * self.z..][..self.z]
    }
}

impl<T> IndexMut<(usize, usize)> for Vec3<T> {
    fn index_mut(&mut self, (x, y): (usize, usize)) -> &mut Self::Output {
        &mut self.v[x * self.y * self.z + y * self.z..][..self.z]
    }
}

impl<T> Index<(usize, usize, usize)> for Vec3<T> {
    type Output = T;

    fn index(&self, (x, y, z): (usize, usize, usize)) -> &Self::Output {
        &self.v[x * self.y * self.z + y * self.z + z]
    }
}

impl<T> IndexMut<(usize, usize, usize)> for Vec3<T> {
    fn index_mut(&mut self, (x, y, z): (usize, usize, usize)) -> &mut Self::Output {
        &mut self.v[x * self.y * self.z + y * self.z + z]
    }
}

impl<T> Deref for Vec3<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.v.deref()
    }
}

impl<T> DerefMut for Vec3<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.v.deref_mut()
    }
}
