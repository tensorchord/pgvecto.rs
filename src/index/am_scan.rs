use crate::error::*;
use crate::gucs::executing::search_options;
use crate::gucs::planning::Mode;
use crate::gucs::planning::SEARCH_MODE;
use crate::ipc::{client, ClientVbase};
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
    Vbase {
        vbase: ClientVbase,
        threshold: Option<f32>,
        recheck: bool,
    },
    Empty {},
}

pub fn scan_build(
    orderbys: Vec<Option<OwnedVector>>,
    spheres: Vec<(Option<OwnedVector>, Option<f32>)>,
) -> (Option<OwnedVector>, Option<f32>, bool) {
    let mut vector = None;
    let mut threshold = None;
    let mut recheck = false;
    for orderby_vector in orderbys {
        if vector.is_none() {
            vector = orderby_vector;
        } else if orderby_vector.is_some() && vector != orderby_vector {
            pgrx::error!("vector search with multiple vectors is not supported");
        }
    }
    for (sphere_vector, sphere_threshold) in spheres {
        if vector.is_none() {
            vector = sphere_vector;
            threshold = sphere_threshold;
        } else if vector == sphere_vector {
            if threshold.is_none() || sphere_threshold < threshold {
                threshold = sphere_threshold;
            }
        } else {
            recheck = true;
            break;
        }
    }
    (vector, threshold, recheck)
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
                Mode::basic | Mode::vbase => {
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
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
        }
        Scanner::Empty {} => {}
    }
}
