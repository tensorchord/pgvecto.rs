use pgrx::pg_sys::{Datum, Oid};
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::FunctionMetadataTypeEntity;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::{FromDatum, IntoDatum};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Rem, RemAssign, Sub, SubAssign};

pub type Float = f32;

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Scalar(pub Float);

impl Scalar {
    pub const INFINITY: Self = Self(Float::INFINITY);
    pub const NEG_INFINITY: Self = Self(Float::NEG_INFINITY);
    pub const NAN: Self = Self(Float::NAN);
    pub const Z: Self = Self(0.0);

    #[inline(always)]
    pub fn acos(self) -> Self {
        Self(self.0.acos())
    }

    #[inline(always)]
    pub fn sqrt(self) -> Self {
        Self(self.0.sqrt())
    }
}

impl Debug for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl From<Float> for Scalar {
    fn from(value: Float) -> Self {
        Self(value)
    }
}

impl From<Scalar> for Float {
    fn from(Scalar(float): Scalar) -> Self {
        float
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for Scalar {}

impl PartialOrd for Scalar {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for Scalar {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl Add<Float> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn add(self, rhs: Float) -> Scalar {
        unsafe { std::intrinsics::fadd_fast(self.0, rhs).into() }
    }
}

impl AddAssign<Float> for Scalar {
    fn add_assign(&mut self, rhs: Float) {
        unsafe { self.0 = std::intrinsics::fadd_fast(self.0, rhs) }
    }
}

impl Add<Scalar> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn add(self, rhs: Scalar) -> Scalar {
        unsafe { std::intrinsics::fadd_fast(self.0, rhs.0).into() }
    }
}

impl AddAssign<Scalar> for Scalar {
    #[inline(always)]
    fn add_assign(&mut self, rhs: Scalar) {
        unsafe { self.0 = std::intrinsics::fadd_fast(self.0, rhs.0) }
    }
}

impl Sub<Float> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn sub(self, rhs: Float) -> Scalar {
        unsafe { std::intrinsics::fsub_fast(self.0, rhs).into() }
    }
}

impl SubAssign<Float> for Scalar {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: Float) {
        unsafe { self.0 = std::intrinsics::fsub_fast(self.0, rhs) }
    }
}

impl Sub<Scalar> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn sub(self, rhs: Scalar) -> Scalar {
        unsafe { std::intrinsics::fsub_fast(self.0, rhs.0).into() }
    }
}

impl SubAssign<Scalar> for Scalar {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: Scalar) {
        unsafe { self.0 = std::intrinsics::fsub_fast(self.0, rhs.0) }
    }
}

impl Mul<Float> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn mul(self, rhs: Float) -> Scalar {
        unsafe { std::intrinsics::fmul_fast(self.0, rhs).into() }
    }
}

impl MulAssign<Float> for Scalar {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: Float) {
        unsafe { self.0 = std::intrinsics::fmul_fast(self.0, rhs) }
    }
}

impl Mul<Scalar> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn mul(self, rhs: Scalar) -> Scalar {
        unsafe { std::intrinsics::fmul_fast(self.0, rhs.0).into() }
    }
}

impl MulAssign<Scalar> for Scalar {
    #[inline(always)]
    fn mul_assign(&mut self, rhs: Scalar) {
        unsafe { self.0 = std::intrinsics::fmul_fast(self.0, rhs.0) }
    }
}

impl Div<Float> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn div(self, rhs: Float) -> Scalar {
        unsafe { std::intrinsics::fdiv_fast(self.0, rhs).into() }
    }
}

impl DivAssign<Float> for Scalar {
    #[inline(always)]
    fn div_assign(&mut self, rhs: Float) {
        unsafe { self.0 = std::intrinsics::fdiv_fast(self.0, rhs) }
    }
}

impl Div<Scalar> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn div(self, rhs: Scalar) -> Scalar {
        unsafe { std::intrinsics::fdiv_fast(self.0, rhs.0).into() }
    }
}

impl DivAssign<Scalar> for Scalar {
    #[inline(always)]
    fn div_assign(&mut self, rhs: Scalar) {
        unsafe { self.0 = std::intrinsics::fdiv_fast(self.0, rhs.0) }
    }
}

impl Rem<Float> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn rem(self, rhs: Float) -> Scalar {
        unsafe { std::intrinsics::frem_fast(self.0, rhs).into() }
    }
}

impl RemAssign<Float> for Scalar {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: Float) {
        unsafe { self.0 = std::intrinsics::frem_fast(self.0, rhs) }
    }
}

impl Rem<Scalar> for Scalar {
    type Output = Scalar;

    #[inline(always)]
    fn rem(self, rhs: Scalar) -> Scalar {
        unsafe { std::intrinsics::frem_fast(self.0, rhs.0).into() }
    }
}

impl RemAssign<Scalar> for Scalar {
    #[inline(always)]
    fn rem_assign(&mut self, rhs: Scalar) {
        unsafe { self.0 = std::intrinsics::frem_fast(self.0, rhs.0) }
    }
}

impl FromDatum for Scalar {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self> {
        Float::from_polymorphic_datum(datum, is_null, typoid).map(Self)
    }
}

impl IntoDatum for Scalar {
    fn into_datum(self) -> Option<Datum> {
        Float::into_datum(self.0)
    }

    fn type_oid() -> Oid {
        Float::type_oid()
    }
}

unsafe impl SqlTranslatable for Scalar {
    fn type_name() -> &'static str {
        Float::type_name()
    }
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Float::argument_sql()
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Float::return_sql()
    }
    fn variadic() -> bool {
        Float::variadic()
    }
    fn optional() -> bool {
        Float::optional()
    }
    fn entity() -> FunctionMetadataTypeEntity {
        Float::entity()
    }
}
