use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust, FromDatum, IntoDatum, UnboxDatum};

pub const BALL_ATTR_SOURCE: &str = "source";
pub const BALL_ATTR_THRESHOLD: &str = "threshold";

pub const BALL_VECF32: &str = "ball_vector";
pub const BALL_VECF16: &str = "ball_vecf16";
pub const BALL_SVECF32: &str = "ball_svector";
pub const BALL_BVECF32: &str = "ball_bvector";
pub const BALL_VECI8: &str = "ball_veci8";

#[derive(Debug, Clone)]
pub struct PushdownRange {
    pub threshold: f32,
}

impl PushdownRange {
    pub fn filter(&self, distance: f32) -> bool {
        distance < self.threshold
    }
}

pub fn composite_get<'tup, T>(rhs: &'tup PgHeapTuple<AllocatedByRust>, attribute: &str) -> T
where
    T: FromDatum + IntoDatum + UnboxDatum<As<'tup> = T> + 'tup,
{
    match rhs.get_by_name::<T>(attribute) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty {attribute} at record"),
        Err(e) => pgrx::error!("Parse {attribute} failed at record:{e}"),
    }
}
