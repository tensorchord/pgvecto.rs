use crate::error::*;
use crate::gucs::executing::search_options;
use crate::gucs::planning::Mode;
use crate::gucs::planning::SEARCH_MODE;
use crate::ipc::{client, ClientBasic, ClientVbase};
use base::index::*;
use base::scalar::F32;
use base::search::*;
use base::vector::*;

use super::am_options;
use super::utils::from_datum_to_range;
use super::utils::from_datum_to_vector;

pub enum Scanner {
    Initial {
        vector: Option<OwnedVector>,
        threshold: Option<f32>,
        recheck: bool,
    },
    Basic {
        basic: ClientBasic,
        threshold: Option<f32>,
        recheck: bool,
    },
    Vbase {
        vbase: ClientVbase,
        threshold: Option<f32>,
        recheck: bool,
    },
    Empty {},
}

pub fn scan_make(vector: Option<OwnedVector>, threshold: Option<f32>, recheck: bool) -> Scanner {
    Scanner::Initial {
        vector,
        threshold,
        recheck,
    }
}

pub fn scan_next(scanner: &mut Scanner, handle: Handle) -> Option<(Pointer, bool)> {
    if let Scanner::Initial {
        vector,
        threshold,
        recheck,
    } = scanner
    {
        if let Some(vector) = vector.as_ref() {
            let rpc = check_client(client());

            match SEARCH_MODE.get() {
                Mode::basic => {
                    let opts = search_options();
                    let basic = match rpc.basic(handle, vector.clone(), opts) {
                        Ok(x) => x,
                        Err((_, BasicError::NotExist)) => bad_service_not_exist(),
                        Err((_, BasicError::InvalidVector)) => bad_service_invalid_vector(),
                        Err((_, BasicError::InvalidSearchOptions { reason: _ })) => unreachable!(),
                    };
                    *scanner = Scanner::Basic {
                        basic,
                        threshold: *threshold,
                        recheck: *recheck,
                    };
                }
                Mode::vbase => {
                    let opts = search_options();
                    let vbase = match rpc.vbase(handle, vector.clone(), opts) {
                        Ok(x) => x,
                        Err((_, VbaseError::NotExist)) => bad_service_not_exist(),
                        Err((_, VbaseError::InvalidVector)) => bad_service_invalid_vector(),
                        Err((_, VbaseError::InvalidSearchOptions { reason: _ })) => unreachable!(),
                    };
                    *scanner = Scanner::Vbase {
                        vbase,
                        threshold: *threshold,
                        recheck: *recheck,
                    };
                }
            }
        } else {
            *scanner = Scanner::Empty {};
        }
    }
    let (result, threshold, recheck) = match scanner {
        Scanner::Initial { .. } => unreachable!(),
        Scanner::Basic {
            basic,
            threshold,
            recheck,
        } => (basic.next(), *threshold, *recheck),
        Scanner::Vbase {
            vbase,
            threshold,
            recheck,
        } => (vbase.next(), *threshold, *recheck),
        Scanner::Empty {} => return None,
    };
    match (result, threshold) {
        (Some((_, ptr)), None) => Some((ptr, recheck)),
        (Some((distance, ptr)), Some(t)) if distance < F32(t) => Some((ptr, recheck)),
        _ => {
            let scanner = std::mem::replace(scanner, Scanner::Empty {});
            scan_release(scanner);
            None
        }
    }
}

pub fn scan_release(scanner: Scanner) {
    match scanner {
        Scanner::Initial { .. } => {}
        Scanner::Basic { basic, .. } => {
            basic.leave();
        }
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
        }
        Scanner::Empty {} => {}
    }
}

pub fn fetch_scanner_arguments(
    scan: pgrx::pg_sys::IndexScanDesc,
) -> (Option<OwnedVector>, Option<f32>, bool) {
    unsafe {
        let number_of_order_bys = (*scan).numberOfOrderBys;
        let number_of_keys = (*scan).numberOfKeys;

        if number_of_order_bys > 1 {
            pgrx::error!("vector search with multiple ORDER BY clauses is not supported");
        }
        if number_of_order_bys == 0 && number_of_keys == 0 {
            pgrx::error!(
                "vector search with no WHERE clause and no ORDER BY clause is not supported"
            );
        }

        match (number_of_order_bys, number_of_keys) {
            (0, 1) => {
                let data = (*scan).keyData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                let (options, _) = am_options::options((*scan).indexRelation);
                let (key_vector, threshold) = from_datum_to_range(value, &options.vector, is_null);
                (key_vector, threshold, false)
            }
            (0, n) => {
                // Pick the tightest threshold by first key vector
                let mut vector: Option<OwnedVector> = None;
                let mut threshold: Option<f32> = None;

                let (options, _) = am_options::options((*scan).indexRelation);
                for i in 0..n as usize {
                    let data = (*scan).keyData.add(i);
                    if (*data).sk_strategy != 2 {
                        continue;
                    }
                    let value = (*data).sk_argument;
                    let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                    let (v, t) = from_datum_to_range(value, &options.vector, is_null);
                    if (vector.is_some() && vector != v) || v.is_none() || t.is_none() {
                        continue;
                    }
                    match (threshold, t) {
                        (None, _) => (vector, threshold) = (v, t),
                        (Some(old), Some(new)) if new < old => (vector, threshold) = (v, t),
                        _ => {}
                    }
                }
                (vector, threshold, true)
            }
            (1, 0) => {
                let data = (*scan).orderByData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                let orderby_vector = from_datum_to_vector(value, is_null);
                (orderby_vector, None, false)
            }
            (1, 1) => {
                let data = (*scan).keyData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                let (options, _) = am_options::options((*scan).indexRelation);
                let (key_vector, threshold) = from_datum_to_range(value, &options.vector, is_null);

                let data = (*scan).orderByData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                let orderby_vector = from_datum_to_vector(value, is_null);
                if key_vector == orderby_vector {
                    (key_vector, threshold, false)
                } else {
                    (None, None, true)
                }
            }
            (1, n) => {
                // Pick the tightest threshold by orderby vector
                let data = (*scan).orderByData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                let orderby_vector = from_datum_to_vector(value, is_null);

                let mut threshold: Option<f32> = None;

                let (options, _) = am_options::options((*scan).indexRelation);
                for i in 0..n as usize {
                    let data = (*scan).keyData.add(i);
                    if (*data).sk_strategy != 2 {
                        continue;
                    }
                    let value = (*data).sk_argument;
                    let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                    let (v, t) = from_datum_to_range(value, &options.vector, is_null);
                    if v != orderby_vector || t.is_none() {
                        continue;
                    }
                    match (threshold, t) {
                        (None, _) => threshold = t,
                        (Some(old), Some(new)) if new < old => threshold = t,
                        _ => {}
                    }
                }
                (orderby_vector, threshold, true)
            }
            _ => unreachable!(),
        }
    }
}
