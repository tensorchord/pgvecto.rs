use crate::error::*;
use crate::gucs::executing::search_options;
use crate::gucs::planning::Mode;
use crate::gucs::planning::SEARCH_MODE;
use crate::ipc::{client, ClientBasic, ClientVbase};
use base::index::*;
use base::scalar::F32;
use base::search::*;
use base::vector::*;

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
    match scanner {
        Scanner::Initial { .. } => unreachable!(),
        Scanner::Basic {
            basic,
            threshold,
            recheck,
        } => match (basic.next(), threshold) {
            (Some((_, ptr)), None) => Some((ptr, *recheck)),
            (Some((distance, ptr)), Some(t)) if distance < F32(*t) => Some((ptr, *recheck)),
            _ => {
                let scanner = std::mem::replace(scanner, Scanner::Empty {});
                scan_release(scanner);
                None
            }
        },
        Scanner::Vbase {
            vbase,
            threshold,
            recheck,
        } => match (vbase.next(), threshold) {
            (Some((_, ptr)), None) => Some((ptr, *recheck)),
            (Some((distance, ptr)), Some(t)) if distance < F32(*t) => Some((ptr, *recheck)),
            _ => {
                let scanner = std::mem::replace(scanner, Scanner::Empty {});
                scan_release(scanner);
                None
            }
        },
        Scanner::Empty {} => None,
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
    order_bys: Vec<Option<OwnedVector>>,
    keys: Vec<(Option<OwnedVector>, Option<f32>)>,
) -> (Option<OwnedVector>, Option<f32>, bool) {
    let mut vector = order_bys.into_iter().next().flatten();
    let mut threshold = None;
    let mut recheck = false;

    for (range_vector, range_threshold) in keys {
        if vector.is_none() {
            (vector, threshold) = (range_vector, range_threshold);
        } else if vector == range_vector {
            match (threshold, range_threshold) {
                (None, _) => threshold = range_threshold,
                (Some(old), Some(new)) if new < old => threshold = range_threshold,
                _ => {}
            }
        } else {
            recheck = true;
        }
    }
    (vector, threshold, recheck)
}
