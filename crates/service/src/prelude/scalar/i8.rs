use crate::prelude::global::FloatCast;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::num::ParseIntError;
use std::ops::*;
use std::str::FromStr;

use super::F32;

// TODO: lots of useless code

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct I8(pub i8);

impl Debug for I8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for I8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl PartialEq for I8 {
    fn eq(&self, other: &Self) -> bool {
        self.0.cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for I8 {}

impl PartialOrd for I8 {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for I8 {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

unsafe impl bytemuck::Zeroable for I8 {}

unsafe impl bytemuck::Pod for I8 {}

impl num_traits::Zero for I8 {
    fn zero() -> Self {
        Self(i8::zero())
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl num_traits::One for I8 {
    fn one() -> Self {
        Self(i8::one())
    }
}

impl num_traits::FromPrimitive for I8 {
    fn from_i64(n: i64) -> Option<Self> {
        i8::from_i64(n).map(Self)
    }

    fn from_u64(n: u64) -> Option<Self> {
        i8::from_u64(n).map(Self)
    }

    fn from_isize(n: isize) -> Option<Self> {
        i8::from_isize(n).map(Self)
    }

    fn from_i8(n: i8) -> Option<Self> {
        i8::from_i8(n).map(Self)
    }

    fn from_i16(n: i16) -> Option<Self> {
        i8::from_i16(n).map(Self)
    }

    fn from_i32(n: i32) -> Option<Self> {
        i8::from_i32(n).map(Self)
    }

    fn from_i128(n: i128) -> Option<Self> {
        i8::from_i128(n).map(Self)
    }

    fn from_usize(n: usize) -> Option<Self> {
        i8::from_usize(n).map(Self)
    }

    fn from_u8(n: u8) -> Option<Self> {
        i8::from_u8(n).map(Self)
    }

    fn from_u16(n: u16) -> Option<Self> {
        i8::from_u16(n).map(Self)
    }

    fn from_u32(n: u32) -> Option<Self> {
        i8::from_u32(n).map(Self)
    }

    fn from_u128(n: u128) -> Option<Self> {
        i8::from_u128(n).map(Self)
    }

    fn from_f32(n: f32) -> Option<Self> {
        i8::from_f32(n).map(Self)
    }

    fn from_f64(n: f64) -> Option<Self> {
        i8::from_f64(n).map(Self)
    }
}

impl num_traits::ToPrimitive for I8 {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }

    fn to_isize(&self) -> Option<isize> {
        self.0.to_isize()
    }

    fn to_i8(&self) -> Option<i8> {
        self.0.to_i8()
    }

    fn to_i16(&self) -> Option<i16> {
        self.0.to_i16()
    }

    fn to_i32(&self) -> Option<i32> {
        self.0.to_i32()
    }

    fn to_i128(&self) -> Option<i128> {
        self.0.to_i128()
    }

    fn to_usize(&self) -> Option<usize> {
        self.0.to_usize()
    }

    fn to_u8(&self) -> Option<u8> {
        self.0.to_u8()
    }

    fn to_u16(&self) -> Option<u16> {
        self.0.to_u16()
    }

    fn to_u32(&self) -> Option<u32> {
        self.0.to_u32()
    }

    fn to_u128(&self) -> Option<u128> {
        self.0.to_u128()
    }

    fn to_f32(&self) -> Option<f32> {
        self.0.to_f32()
    }

    fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }
}

impl num_traits::NumCast for I8 {
    fn from<T: num_traits::ToPrimitive>(n: T) -> Option<Self> {
        num_traits::NumCast::from(n).map(Self)
    }
}

impl num_traits::Num for I8 {
    type FromStrRadixErr = <i8 as num_traits::Num>::FromStrRadixErr;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        i8::from_str_radix(str, radix).map(Self)
    }
}

impl Add<I8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn add(self, rhs: I8) -> Self::Output {
        Self(self.0.add(rhs.0))
    }
}

impl AddAssign<I8> for I8 {
    #[inline(always)]
    fn add_assign(&mut self, rhs: I8) {
        self.0.add_assign(rhs.0)
    }
}

impl Sub<I8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn sub(self, rhs: I8) -> Self::Output {
        Self(self.0.sub(rhs.0))
    }
}

impl SubAssign<I8> for I8 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: I8) {
        self.0.sub_assign(rhs.0)
    }
}

impl Mul<I8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn mul(self, rhs: I8) -> Self::Output {
        Self(self.0.mul(rhs.0))
    }
}

impl MulAssign<I8> for I8 {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: I8) {
        self.0.mul_assign(rhs.0)
    }
}

//Todo@wy: there are codes use scalar division with f32, need to check whether it is correct for i8
impl Div<I8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn div(self, rhs: I8) -> Self::Output {
        Self(self.0.div(rhs.0))
    }
}

impl DivAssign<I8> for I8 {
    #[inline(always)]
    fn div_assign(&mut self, rhs: I8) {
        self.0.div_assign(rhs.0)
    }
}

impl Rem<I8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn rem(self, rhs: I8) -> Self::Output {
        Self(self.0.rem(rhs.0))
    }
}

impl RemAssign<I8> for I8 {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: I8) {
        self.0.rem_assign(rhs.0)
    }
}

impl Neg for I8 {
    type Output = I8;

    #[inline(always)]
    fn neg(self) -> Self::Output {
        Self(self.0.neg())
    }
}

impl FromStr for I8 {
    type Err = ParseIntError;

    #[inline(always)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        i8::from_str(s).map(|x| x.into())
    }
}

impl FloatCast for I8 {
    fn from_f32(x: f32) -> Self {
        Self(x as i8)
    }

    fn to_f32(self) -> f32 {
        self.0 as f32
    }
}

impl From<i8> for I8 {
    fn from(value: i8) -> Self {
        Self(value)
    }
}

impl From<I8> for i8 {
    fn from(I8(value): I8) -> Self {
        value
    }
}

impl From<F32> for I8 {
    fn from(F32(value): F32) -> Self {
        // Because F32 may be out of range of i8 [-128, 127], so we can't use to_int_unchecked here.
        Self(value as i8)
    }
}

impl Add<i8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn add(self, rhs: i8) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl AddAssign<i8> for I8 {
    #[inline(always)]
    fn add_assign(&mut self, rhs: i8) {
        self.0.add_assign(rhs)
    }
}

impl Sub<i8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn sub(self, rhs: i8) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl SubAssign<i8> for I8 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: i8) {
        self.0.sub_assign(rhs)
    }
}

impl Mul<i8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn mul(self, rhs: i8) -> Self::Output {
        Self(self.0.mul(rhs))
    }
}

impl MulAssign<i8> for I8 {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: i8) {
        self.0.mul_assign(rhs)
    }
}

impl Div<i8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn div(self, rhs: i8) -> Self::Output {
        Self(self.0.div(rhs))
    }
}

impl DivAssign<i8> for I8 {
    #[inline(always)]
    fn div_assign(&mut self, rhs: i8) {
        self.0.div_assign(rhs)
    }
}

impl Rem<i8> for I8 {
    type Output = I8;

    #[inline(always)]
    fn rem(self, rhs: i8) -> Self::Output {
        Self(self.0.rem(rhs))
    }
}

impl RemAssign<i8> for I8 {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: i8) {
        self.0.rem_assign(rhs)
    }
}
