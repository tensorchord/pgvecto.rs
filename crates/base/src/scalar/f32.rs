use super::ScalarLike;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::iter::Sum;
use std::num::ParseFloatError;
use std::ops::*;
use std::str::FromStr;

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct F32(pub f32);

impl Debug for F32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for F32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl PartialEq for F32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for F32 {}

impl PartialOrd for F32 {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for F32 {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

unsafe impl bytemuck::Zeroable for F32 {}

unsafe impl bytemuck::Pod for F32 {}

impl num_traits::Zero for F32 {
    fn zero() -> Self {
        Self(f32::zero())
    }

    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl num_traits::One for F32 {
    fn one() -> Self {
        Self(f32::one())
    }
}

impl num_traits::FromPrimitive for F32 {
    fn from_i64(n: i64) -> Option<Self> {
        f32::from_i64(n).map(Self)
    }

    fn from_u64(n: u64) -> Option<Self> {
        f32::from_u64(n).map(Self)
    }

    fn from_isize(n: isize) -> Option<Self> {
        f32::from_isize(n).map(Self)
    }

    fn from_i8(n: i8) -> Option<Self> {
        f32::from_i8(n).map(Self)
    }

    fn from_i16(n: i16) -> Option<Self> {
        f32::from_i16(n).map(Self)
    }

    fn from_i32(n: i32) -> Option<Self> {
        f32::from_i32(n).map(Self)
    }

    fn from_i128(n: i128) -> Option<Self> {
        f32::from_i128(n).map(Self)
    }

    fn from_usize(n: usize) -> Option<Self> {
        f32::from_usize(n).map(Self)
    }

    fn from_u8(n: u8) -> Option<Self> {
        f32::from_u8(n).map(Self)
    }

    fn from_u16(n: u16) -> Option<Self> {
        f32::from_u16(n).map(Self)
    }

    fn from_u32(n: u32) -> Option<Self> {
        f32::from_u32(n).map(Self)
    }

    fn from_u128(n: u128) -> Option<Self> {
        f32::from_u128(n).map(Self)
    }

    fn from_f32(n: f32) -> Option<Self> {
        f32::from_f32(n).map(Self)
    }

    fn from_f64(n: f64) -> Option<Self> {
        f32::from_f64(n).map(Self)
    }
}

impl num_traits::ToPrimitive for F32 {
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

impl num_traits::NumCast for F32 {
    fn from<T: num_traits::ToPrimitive>(n: T) -> Option<Self> {
        num_traits::NumCast::from(n).map(Self)
    }
}

impl num_traits::Num for F32 {
    type FromStrRadixErr = <f32 as num_traits::Num>::FromStrRadixErr;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        f32::from_str_radix(str, radix).map(Self)
    }
}

impl num_traits::Float for F32 {
    fn nan() -> Self {
        Self(f32::nan())
    }

    fn infinity() -> Self {
        Self(f32::infinity())
    }

    fn neg_infinity() -> Self {
        Self(f32::neg_infinity())
    }

    fn neg_zero() -> Self {
        Self(f32::neg_zero())
    }

    fn min_value() -> Self {
        Self(f32::min_value())
    }

    fn min_positive_value() -> Self {
        Self(f32::min_positive_value())
    }

    fn max_value() -> Self {
        Self(f32::max_value())
    }

    fn is_nan(self) -> bool {
        self.0.is_nan()
    }

    fn is_infinite(self) -> bool {
        self.0.is_infinite()
    }

    fn is_finite(self) -> bool {
        self.0.is_finite()
    }

    fn is_normal(self) -> bool {
        self.0.is_normal()
    }

    fn classify(self) -> std::num::FpCategory {
        self.0.classify()
    }

    fn floor(self) -> Self {
        Self(self.0.floor())
    }

    fn ceil(self) -> Self {
        Self(self.0.ceil())
    }

    fn round(self) -> Self {
        Self(self.0.round())
    }

    fn trunc(self) -> Self {
        Self(self.0.trunc())
    }

    fn fract(self) -> Self {
        Self(self.0.fract())
    }

    fn abs(self) -> Self {
        Self(self.0.abs())
    }

    fn signum(self) -> Self {
        Self(self.0.signum())
    }

    fn is_sign_positive(self) -> bool {
        self.0.is_sign_positive()
    }

    fn is_sign_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    fn mul_add(self, a: Self, b: Self) -> Self {
        Self(self.0.mul_add(a.0, b.0))
    }

    fn recip(self) -> Self {
        Self(self.0.recip())
    }

    fn powi(self, n: i32) -> Self {
        Self(self.0.powi(n))
    }

    fn powf(self, n: Self) -> Self {
        Self(self.0.powf(n.0))
    }

    fn sqrt(self) -> Self {
        Self(self.0.sqrt())
    }

    fn exp(self) -> Self {
        Self(self.0.exp())
    }

    fn exp2(self) -> Self {
        Self(self.0.exp2())
    }

    fn ln(self) -> Self {
        Self(self.0.ln())
    }

    fn log(self, base: Self) -> Self {
        Self(self.0.log(base.0))
    }

    fn log2(self) -> Self {
        Self(self.0.log2())
    }

    fn log10(self) -> Self {
        Self(self.0.log10())
    }

    fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    fn abs_sub(self, _: Self) -> Self {
        unimplemented!()
    }

    fn cbrt(self) -> Self {
        Self(self.0.cbrt())
    }

    fn hypot(self, other: Self) -> Self {
        Self(self.0.hypot(other.0))
    }

    fn sin(self) -> Self {
        Self(self.0.sin())
    }

    fn cos(self) -> Self {
        Self(self.0.cos())
    }

    fn tan(self) -> Self {
        Self(self.0.tan())
    }

    fn asin(self) -> Self {
        Self(self.0.asin())
    }

    fn acos(self) -> Self {
        Self(self.0.acos())
    }

    fn atan(self) -> Self {
        Self(self.0.atan())
    }

    fn atan2(self, other: Self) -> Self {
        Self(self.0.atan2(other.0))
    }

    fn sin_cos(self) -> (Self, Self) {
        let (_x, _y) = self.0.sin_cos();
        (Self(_x), Self(_y))
    }

    fn exp_m1(self) -> Self {
        Self(self.0.exp_m1())
    }

    fn ln_1p(self) -> Self {
        Self(self.0.ln_1p())
    }

    fn sinh(self) -> Self {
        Self(self.0.sinh())
    }

    fn cosh(self) -> Self {
        Self(self.0.cosh())
    }

    fn tanh(self) -> Self {
        Self(self.0.tanh())
    }

    fn asinh(self) -> Self {
        Self(self.0.asinh())
    }

    fn acosh(self) -> Self {
        Self(self.0.acosh())
    }

    fn atanh(self) -> Self {
        Self(self.0.atanh())
    }

    fn integer_decode(self) -> (u64, i16, i8) {
        self.0.integer_decode()
    }

    fn epsilon() -> Self {
        Self(f32::EPSILON)
    }

    fn is_subnormal(self) -> bool {
        self.0.classify() == std::num::FpCategory::Subnormal
    }

    fn to_degrees(self) -> Self {
        Self(self.0.to_degrees())
    }

    fn to_radians(self) -> Self {
        Self(self.0.to_radians())
    }

    fn copysign(self, sign: Self) -> Self {
        Self(self.0.copysign(sign.0))
    }
}

impl Add<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn add(self, rhs: F32) -> F32 {
        unsafe { std::intrinsics::fadd_fast(self.0, rhs.0).into() }
    }
}

impl AddAssign<F32> for F32 {
    #[inline(always)]
    fn add_assign(&mut self, rhs: F32) {
        unsafe { self.0 = std::intrinsics::fadd_fast(self.0, rhs.0) }
    }
}

impl Sum for F32 {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(F32(0.0), Add::add)
    }
}

impl Sub<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn sub(self, rhs: F32) -> F32 {
        unsafe { std::intrinsics::fsub_fast(self.0, rhs.0).into() }
    }
}

impl SubAssign<F32> for F32 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: F32) {
        unsafe { self.0 = std::intrinsics::fsub_fast(self.0, rhs.0) }
    }
}

impl Mul<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn mul(self, rhs: F32) -> F32 {
        unsafe { std::intrinsics::fmul_fast(self.0, rhs.0).into() }
    }
}

impl MulAssign<F32> for F32 {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: F32) {
        unsafe { self.0 = std::intrinsics::fmul_fast(self.0, rhs.0) }
    }
}

impl Div<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn div(self, rhs: F32) -> F32 {
        unsafe { std::intrinsics::fdiv_fast(self.0, rhs.0).into() }
    }
}

impl DivAssign<F32> for F32 {
    #[inline(always)]
    fn div_assign(&mut self, rhs: F32) {
        unsafe { self.0 = std::intrinsics::fdiv_fast(self.0, rhs.0) }
    }
}

impl Rem<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn rem(self, rhs: F32) -> F32 {
        unsafe { std::intrinsics::frem_fast(self.0, rhs.0).into() }
    }
}

impl RemAssign<F32> for F32 {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: F32) {
        unsafe { self.0 = std::intrinsics::frem_fast(self.0, rhs.0) }
    }
}

impl Neg for F32 {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self(self.0.neg())
    }
}

impl FromStr for F32 {
    type Err = ParseFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        f32::from_str(s).map(|x| x.into())
    }
}

impl From<f32> for F32 {
    fn from(value: f32) -> Self {
        Self(value)
    }
}

impl From<F32> for f32 {
    fn from(F32(float): F32) -> Self {
        float
    }
}

impl Add<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn add(self, rhs: f32) -> F32 {
        unsafe { std::intrinsics::fadd_fast(self.0, rhs).into() }
    }
}

impl AddAssign<f32> for F32 {
    fn add_assign(&mut self, rhs: f32) {
        unsafe { self.0 = std::intrinsics::fadd_fast(self.0, rhs) }
    }
}

impl Sub<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn sub(self, rhs: f32) -> F32 {
        unsafe { std::intrinsics::fsub_fast(self.0, rhs).into() }
    }
}

impl SubAssign<f32> for F32 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: f32) {
        unsafe { self.0 = std::intrinsics::fsub_fast(self.0, rhs) }
    }
}

impl Mul<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn mul(self, rhs: f32) -> F32 {
        unsafe { std::intrinsics::fmul_fast(self.0, rhs).into() }
    }
}

impl MulAssign<f32> for F32 {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: f32) {
        unsafe { self.0 = std::intrinsics::fmul_fast(self.0, rhs) }
    }
}

impl Div<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn div(self, rhs: f32) -> F32 {
        unsafe { std::intrinsics::fdiv_fast(self.0, rhs).into() }
    }
}

impl DivAssign<f32> for F32 {
    #[inline(always)]
    fn div_assign(&mut self, rhs: f32) {
        unsafe { self.0 = std::intrinsics::fdiv_fast(self.0, rhs) }
    }
}

impl Rem<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn rem(self, rhs: f32) -> F32 {
        unsafe { std::intrinsics::frem_fast(self.0, rhs).into() }
    }
}

impl RemAssign<f32> for F32 {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: f32) {
        unsafe { self.0 = std::intrinsics::frem_fast(self.0, rhs) }
    }
}

impl ScalarLike for F32 {
    fn from_f32(x: f32) -> Self {
        Self(x)
    }

    fn to_f32(self) -> f32 {
        self.0
    }

    fn from_f(x: F32) -> Self {
        Self::from_f32(x.0)
    }

    fn to_f(self) -> F32 {
        F32(Self::to_f32(self))
    }
}
