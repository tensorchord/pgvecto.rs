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
    pub const ZERO: Self = Self(0);
    pub const INFINITY: Self = Self(2139095040);
    pub const NEG_INFINITY: Self = Self(-2139095041);

    pub fn to_f32(self) -> f32 {
        self.into()
    }
}

impl From<f32> for Distance {
    #[inline(always)]
    fn from(value: f32) -> Self {
        let bits = value.to_bits() as i32;
        Self(bits ^ (((bits >> 31) as u32) >> 1) as i32)
    }
}

impl From<Distance> for f32 {
    #[inline(always)]
    fn from(Distance(bits): Distance) -> Self {
        f32::from_bits((bits ^ (((bits >> 31) as u32) >> 1) as i32) as u32)
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
