use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust, FromDatum, IntoDatum, UnboxDatum};

pub const RELDIS_SOURCE: &str = "source";
pub const RELDIS_OPERATOR: &str = "operator";
pub const RELDIS_THRESHOLD: &str = "threshold";

pub const RELDIS_NAME_VECF32: &str = "relative_distance_vecf32";
pub const RELDIS_NAME_VECF16: &str = "relative_distance_vecf16";
pub const RELDIS_NAME_SVECF32: &str = "relative_distance_svecf32";
pub const RELDIS_NAME_BVECF32: &str = "relative_distance_bvecf32";
pub const RELDIS_NAME_VECI8: &str = "relative_distance_veci8";

#[derive(Debug, PartialEq, Clone)]
pub enum RangeOperator {
    MeasureLess,
    MeasureLessEqual,
}

#[derive(Debug, Clone)]
pub struct PushdownRange {
    pub operator: RangeOperator,
    pub threshold: f32,
}

impl PushdownRange {
    pub fn filter(&self, distance: f32) -> bool {
        match self.operator {
            RangeOperator::MeasureLess => distance < self.threshold,
            RangeOperator::MeasureLessEqual => distance <= self.threshold,
        }
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
