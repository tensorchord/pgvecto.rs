use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DistanceKind {
    L2,
    Dot,
    Hamming,
    Jaccard,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Distance(i32);

impl Distance {
    pub const ZERO: Self = Distance::from_f32(0.0f32);
    pub const INFINITY: Self = Distance::from_f32(f32::INFINITY);
    pub const NEG_INFINITY: Self = Distance::from_f32(f32::NEG_INFINITY);

    #[inline(always)]
    pub const fn from_f32(value: f32) -> Self {
        let bits = value.to_bits() as i32;
        let mask = ((bits >> 31) as u32) >> 1;
        let res = bits ^ (mask as i32);
        Self(res)
    }

    #[inline(always)]
    pub const fn to_f32(self) -> f32 {
        let bits = self.0;
        let mask = ((bits >> 31) as u32) >> 1;
        let res = bits ^ (mask as i32);
        f32::from_bits(res as u32)
    }

    #[inline(always)]
    pub const fn to_i32(self) -> i32 {
        self.0
    }
}

impl From<f32> for Distance {
    #[inline(always)]
    fn from(value: f32) -> Self {
        Distance::from_f32(value)
    }
}

impl From<Distance> for f32 {
    #[inline(always)]
    fn from(value: Distance) -> Self {
        Distance::to_f32(value)
    }
}

#[test]
fn distance_conversions() {
    assert_eq!(Distance::from(0.0f32), Distance::ZERO);
    assert_eq!(Distance::from(f32::INFINITY), Distance::INFINITY);
    assert_eq!(Distance::from(f32::NEG_INFINITY), Distance::NEG_INFINITY);
    for i in -100..100 {
        let val = (i as f32) * 0.1;
        assert_eq!(f32::from(Distance::from(val)).to_bits(), val.to_bits());
    }
    assert_eq!(
        f32::from(Distance::from(0.0f32)).to_bits(),
        0.0f32.to_bits()
    );
    assert_eq!(
        f32::from(Distance::from(-0.0f32)).to_bits(),
        (-0.0f32).to_bits()
    );
    assert_eq!(
        f32::from(Distance::from(f32::NAN)).to_bits(),
        f32::NAN.to_bits()
    );
    assert_eq!(
        f32::from(Distance::from(-f32::NAN)).to_bits(),
        (-f32::NAN).to_bits()
    );
    assert_eq!(
        f32::from(Distance::from(f32::INFINITY)).to_bits(),
        f32::INFINITY.to_bits()
    );
    assert_eq!(
        f32::from(Distance::from(-f32::INFINITY)).to_bits(),
        (-f32::INFINITY).to_bits()
    );
}
