use std::num::NonZero;

use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust, FromDatum, IntoDatum, UnboxDatum};

#[inline(always)]
pub fn composite_get<'tup, T>(rhs: &'tup PgHeapTuple<AllocatedByRust>, attno: usize) -> T
where
    T: FromDatum + IntoDatum + UnboxDatum<As<'tup> = T> + 'tup,
{
    let no = NonZero::new(attno).unwrap();
    match rhs.get_by_index::<T>(no) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty index {attno} at record"),
        Err(e) => pgrx::error!("Parse index{attno} failed at record:{e}"),
    }
}
