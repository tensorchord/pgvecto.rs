use super::ScalarLike;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::iter::Sum;
use std::num::ParseFloatError;
use std::ops::*;
use std::str::FromStr;

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct F32(pub f32);

impl Hash for F32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use num_traits::{Float, Zero};
        let bits = if self.is_nan() {
            f32::NAN.to_bits()
        } else if self.is_zero() {
            f32::zero().to_bits()
        } else {
            self.0.to_bits()
        };
        bits.hash(state)
    }
}

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
    #[inline(always)]
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

impl Zero for F32 {
    #[inline(always)]
    fn zero() -> Self {
        Self(f32::zero())
    }

    #[inline(always)]
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl num_traits::One for F32 {
    #[inline(always)]
    fn one() -> Self {
        Self(f32::one())
    }
}

impl num_traits::FromPrimitive for F32 {
    #[inline(always)]
    fn from_i64(n: i64) -> Option<Self> {
        f32::from_i64(n).map(Self)
    }

    #[inline(always)]
    fn from_u64(n: u64) -> Option<Self> {
        f32::from_u64(n).map(Self)
    }

    #[inline(always)]
    fn from_isize(n: isize) -> Option<Self> {
        f32::from_isize(n).map(Self)
    }

    #[inline(always)]
    fn from_i8(n: i8) -> Option<Self> {
        f32::from_i8(n).map(Self)
    }

    #[inline(always)]
    fn from_i16(n: i16) -> Option<Self> {
        f32::from_i16(n).map(Self)
    }

    #[inline(always)]
    fn from_i32(n: i32) -> Option<Self> {
        f32::from_i32(n).map(Self)
    }

    #[inline(always)]
    fn from_i128(n: i128) -> Option<Self> {
        f32::from_i128(n).map(Self)
    }

    #[inline(always)]
    fn from_usize(n: usize) -> Option<Self> {
        f32::from_usize(n).map(Self)
    }

    #[inline(always)]
    fn from_u8(n: u8) -> Option<Self> {
        f32::from_u8(n).map(Self)
    }

    #[inline(always)]
    fn from_u16(n: u16) -> Option<Self> {
        f32::from_u16(n).map(Self)
    }

    #[inline(always)]
    fn from_u32(n: u32) -> Option<Self> {
        f32::from_u32(n).map(Self)
    }

    #[inline(always)]
    fn from_u128(n: u128) -> Option<Self> {
        f32::from_u128(n).map(Self)
    }

    #[inline(always)]
    fn from_f32(n: f32) -> Option<Self> {
        f32::from_f32(n).map(Self)
    }

    #[inline(always)]
    fn from_f64(n: f64) -> Option<Self> {
        f32::from_f64(n).map(Self)
    }
}

impl num_traits::ToPrimitive for F32 {
    #[inline(always)]
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    #[inline(always)]
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }

    #[inline(always)]
    fn to_isize(&self) -> Option<isize> {
        self.0.to_isize()
    }

    #[inline(always)]
    fn to_i8(&self) -> Option<i8> {
        self.0.to_i8()
    }

    #[inline(always)]
    fn to_i16(&self) -> Option<i16> {
        self.0.to_i16()
    }

    #[inline(always)]
    fn to_i32(&self) -> Option<i32> {
        self.0.to_i32()
    }

    #[inline(always)]
    fn to_i128(&self) -> Option<i128> {
        self.0.to_i128()
    }

    #[inline(always)]
    fn to_usize(&self) -> Option<usize> {
        self.0.to_usize()
    }

    #[inline(always)]
    fn to_u8(&self) -> Option<u8> {
        self.0.to_u8()
    }

    #[inline(always)]
    fn to_u16(&self) -> Option<u16> {
        self.0.to_u16()
    }

    #[inline(always)]
    fn to_u32(&self) -> Option<u32> {
        self.0.to_u32()
    }

    #[inline(always)]
    fn to_u128(&self) -> Option<u128> {
        self.0.to_u128()
    }

    #[inline(always)]
    fn to_f32(&self) -> Option<f32> {
        self.0.to_f32()
    }

    #[inline(always)]
    fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }
}

impl num_traits::NumCast for F32 {
    #[inline(always)]
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
    #[inline(always)]
    fn nan() -> Self {
        Self(f32::nan())
    }

    #[inline(always)]
    fn infinity() -> Self {
        Self(f32::infinity())
    }

    #[inline(always)]
    fn neg_infinity() -> Self {
        Self(f32::neg_infinity())
    }

    #[inline(always)]
    fn neg_zero() -> Self {
        Self(f32::neg_zero())
    }

    #[inline(always)]
    fn min_value() -> Self {
        Self(f32::min_value())
    }

    #[inline(always)]
    fn min_positive_value() -> Self {
        Self(f32::min_positive_value())
    }

    #[inline(always)]
    fn max_value() -> Self {
        Self(f32::max_value())
    }

    #[inline(always)]
    fn is_nan(self) -> bool {
        self.0.is_nan()
    }

    #[inline(always)]
    fn is_infinite(self) -> bool {
        self.0.is_infinite()
    }

    #[inline(always)]
    fn is_finite(self) -> bool {
        self.0.is_finite()
    }

    #[inline(always)]
    fn is_normal(self) -> bool {
        self.0.is_normal()
    }

    #[inline(always)]
    fn classify(self) -> std::num::FpCategory {
        self.0.classify()
    }

    #[inline(always)]
    fn floor(self) -> Self {
        Self(self.0.floor())
    }

    #[inline(always)]
    fn ceil(self) -> Self {
        Self(self.0.ceil())
    }

    #[inline(always)]
    fn round(self) -> Self {
        Self(self.0.round())
    }

    #[inline(always)]
    fn trunc(self) -> Self {
        Self(self.0.trunc())
    }

    #[inline(always)]
    fn fract(self) -> Self {
        Self(self.0.fract())
    }

    #[inline(always)]
    fn abs(self) -> Self {
        Self(self.0.abs())
    }

    #[inline(always)]
    fn signum(self) -> Self {
        Self(self.0.signum())
    }

    #[inline(always)]
    fn is_sign_positive(self) -> bool {
        self.0.is_sign_positive()
    }

    #[inline(always)]
    fn is_sign_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    #[inline(always)]
    fn mul_add(self, a: Self, b: Self) -> Self {
        Self(self.0.mul_add(a.0, b.0))
    }

    #[inline(always)]
    fn recip(self) -> Self {
        Self(self.0.recip())
    }

    #[inline(always)]
    fn powi(self, n: i32) -> Self {
        Self(self.0.powi(n))
    }

    #[inline(always)]
    fn powf(self, n: Self) -> Self {
        Self(self.0.powf(n.0))
    }

    #[inline(always)]
    fn sqrt(self) -> Self {
        Self(self.0.sqrt())
    }

    #[inline(always)]
    fn exp(self) -> Self {
        Self(self.0.exp())
    }

    #[inline(always)]
    fn exp2(self) -> Self {
        Self(self.0.exp2())
    }

    #[inline(always)]
    fn ln(self) -> Self {
        Self(self.0.ln())
    }

    #[inline(always)]
    fn log(self, base: Self) -> Self {
        Self(self.0.log(base.0))
    }

    #[inline(always)]
    fn log2(self) -> Self {
        Self(self.0.log2())
    }

    #[inline(always)]
    fn log10(self) -> Self {
        Self(self.0.log10())
    }

    #[inline(always)]
    fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    #[inline(always)]
    fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    #[inline(always)]
    fn abs_sub(self, _: Self) -> Self {
        unimplemented!()
    }

    #[inline(always)]
    fn cbrt(self) -> Self {
        Self(self.0.cbrt())
    }

    #[inline(always)]
    fn hypot(self, other: Self) -> Self {
        Self(self.0.hypot(other.0))
    }

    #[inline(always)]
    fn sin(self) -> Self {
        Self(self.0.sin())
    }

    #[inline(always)]
    fn cos(self) -> Self {
        Self(self.0.cos())
    }

    #[inline(always)]
    fn tan(self) -> Self {
        Self(self.0.tan())
    }

    #[inline(always)]
    fn asin(self) -> Self {
        Self(self.0.asin())
    }

    #[inline(always)]
    fn acos(self) -> Self {
        Self(self.0.acos())
    }

    #[inline(always)]
    fn atan(self) -> Self {
        Self(self.0.atan())
    }

    #[inline(always)]
    fn atan2(self, other: Self) -> Self {
        Self(self.0.atan2(other.0))
    }

    #[inline(always)]
    fn sin_cos(self) -> (Self, Self) {
        let (_x, _y) = self.0.sin_cos();
        (Self(_x), Self(_y))
    }

    #[inline(always)]
    fn exp_m1(self) -> Self {
        Self(self.0.exp_m1())
    }

    #[inline(always)]
    fn ln_1p(self) -> Self {
        Self(self.0.ln_1p())
    }

    #[inline(always)]
    fn sinh(self) -> Self {
        Self(self.0.sinh())
    }

    #[inline(always)]
    fn cosh(self) -> Self {
        Self(self.0.cosh())
    }

    #[inline(always)]
    fn tanh(self) -> Self {
        Self(self.0.tanh())
    }

    #[inline(always)]
    fn asinh(self) -> Self {
        Self(self.0.asinh())
    }

    #[inline(always)]
    fn acosh(self) -> Self {
        Self(self.0.acosh())
    }

    #[inline(always)]
    fn atanh(self) -> Self {
        Self(self.0.atanh())
    }

    #[inline(always)]
    fn integer_decode(self) -> (u64, i16, i8) {
        self.0.integer_decode()
    }

    #[inline(always)]
    fn epsilon() -> Self {
        Self(f32::EPSILON)
    }

    #[inline(always)]
    fn is_subnormal(self) -> bool {
        self.0.classify() == std::num::FpCategory::Subnormal
    }

    #[inline(always)]
    fn to_degrees(self) -> Self {
        Self(self.0.to_degrees())
    }

    #[inline(always)]
    fn to_radians(self) -> Self {
        Self(self.0.to_radians())
    }

    #[inline(always)]
    fn copysign(self, sign: Self) -> Self {
        Self(self.0.copysign(sign.0))
    }
}

impl Add<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn add(self, rhs: F32) -> F32 {
        intrinsics::fadd_algebraic(self.0, rhs.0).into()
    }
}

impl AddAssign<F32> for F32 {
    #[inline(always)]
    fn add_assign(&mut self, rhs: F32) {
        self.0 = intrinsics::fadd_algebraic(self.0, rhs.0)
    }
}

impl Sum for F32 {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(F32::zero(), Add::add)
    }
}

impl Sub<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn sub(self, rhs: F32) -> F32 {
        intrinsics::fsub_algebraic(self.0, rhs.0).into()
    }
}

impl SubAssign<F32> for F32 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: F32) {
        self.0 = intrinsics::fsub_algebraic(self.0, rhs.0)
    }
}

impl Mul<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn mul(self, rhs: F32) -> F32 {
        intrinsics::fmul_algebraic(self.0, rhs.0).into()
    }
}

impl MulAssign<F32> for F32 {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: F32) {
        self.0 = intrinsics::fmul_algebraic(self.0, rhs.0)
    }
}

impl Div<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn div(self, rhs: F32) -> F32 {
        intrinsics::fdiv_algebraic(self.0, rhs.0).into()
    }
}

impl DivAssign<F32> for F32 {
    #[inline(always)]
    fn div_assign(&mut self, rhs: F32) {
        self.0 = intrinsics::fdiv_algebraic(self.0, rhs.0)
    }
}

impl Rem<F32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn rem(self, rhs: F32) -> F32 {
        intrinsics::frem_algebraic(self.0, rhs.0).into()
    }
}

impl RemAssign<F32> for F32 {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: F32) {
        self.0 = intrinsics::frem_algebraic(self.0, rhs.0)
    }
}

impl Neg for F32 {
    type Output = Self;

    #[inline(always)]
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
    #[inline(always)]
    fn from(value: f32) -> Self {
        Self(value)
    }
}

impl From<F32> for f32 {
    #[inline(always)]
    fn from(F32(float): F32) -> Self {
        float
    }
}

impl Add<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn add(self, rhs: f32) -> F32 {
        intrinsics::fadd_algebraic(self.0, rhs).into()
    }
}

impl AddAssign<f32> for F32 {
    #[inline(always)]
    fn add_assign(&mut self, rhs: f32) {
        self.0 = intrinsics::fadd_algebraic(self.0, rhs)
    }
}

impl Sub<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn sub(self, rhs: f32) -> F32 {
        intrinsics::fsub_algebraic(self.0, rhs).into()
    }
}

impl SubAssign<f32> for F32 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: f32) {
        self.0 = intrinsics::fsub_algebraic(self.0, rhs)
    }
}

impl Mul<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn mul(self, rhs: f32) -> F32 {
        intrinsics::fmul_algebraic(self.0, rhs).into()
    }
}

impl MulAssign<f32> for F32 {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: f32) {
        self.0 = intrinsics::fmul_algebraic(self.0, rhs)
    }
}

impl Div<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn div(self, rhs: f32) -> F32 {
        intrinsics::fdiv_algebraic(self.0, rhs).into()
    }
}

impl DivAssign<f32> for F32 {
    #[inline(always)]
    fn div_assign(&mut self, rhs: f32) {
        self.0 = intrinsics::fdiv_algebraic(self.0, rhs)
    }
}

impl Rem<f32> for F32 {
    type Output = F32;

    #[inline(always)]
    fn rem(self, rhs: f32) -> F32 {
        intrinsics::frem_algebraic(self.0, rhs).into()
    }
}

impl RemAssign<f32> for F32 {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: f32) {
        self.0 = intrinsics::frem_algebraic(self.0, rhs)
    }
}

mod intrinsics {
    #[inline(always)]
    pub fn fadd_algebraic(lhs: f32, rhs: f32) -> f32 {
        std::intrinsics::fadd_algebraic(lhs, rhs)
    }
    #[inline(always)]
    pub fn fsub_algebraic(lhs: f32, rhs: f32) -> f32 {
        std::intrinsics::fsub_algebraic(lhs, rhs)
    }
    #[inline(always)]
    pub fn fmul_algebraic(lhs: f32, rhs: f32) -> f32 {
        std::intrinsics::fmul_algebraic(lhs, rhs)
    }
    #[inline(always)]
    pub fn fdiv_algebraic(lhs: f32, rhs: f32) -> f32 {
        std::intrinsics::fdiv_algebraic(lhs, rhs)
    }
    #[inline(always)]
    pub fn frem_algebraic(lhs: f32, rhs: f32) -> f32 {
        std::intrinsics::frem_algebraic(lhs, rhs)
    }
}

impl ScalarLike for F32 {
    #[inline(always)]
    fn from_f32(x: f32) -> Self {
        Self(x)
    }

    #[inline(always)]
    fn to_f32(self) -> f32 {
        self.0
    }

    #[inline(always)]
    fn from_f(x: F32) -> Self {
        Self::from_f32(x.0)
    }

    #[inline(always)]
    fn to_f(self) -> F32 {
        F32(Self::to_f32(self))
    }
}
