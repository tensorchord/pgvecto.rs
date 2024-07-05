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
    },
    Basic {
        basic: ClientBasic,
        threshold: Option<f32>,
    },
    Vbase {
        vbase: ClientVbase,
        threshold: Option<f32>,
    },
    Empty {
        threshold: Option<f32>,
    },
}

impl Scanner {
    pub fn threshold(&self) -> Option<f32> {
        match self {
            Scanner::Initial {
                vector: _,
                threshold,
            } => *threshold,
            Scanner::Basic {
                basic: _,
                threshold,
            } => *threshold,
            Scanner::Vbase {
                vbase: _,
                threshold,
            } => *threshold,
            Scanner::Empty { threshold } => *threshold,
        }
    }
}

pub fn scan_make(vector: Option<OwnedVector>, threshold: Option<f32>) -> Scanner {
    Scanner::Initial { vector, threshold }
}

pub fn scan_next(scanner: &mut Scanner, handle: Handle) -> Option<(F32, Pointer)> {
    if let Scanner::Initial { vector, threshold } = scanner {
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
                    };
                }
            }
        } else {
            *scanner = Scanner::Empty {
                threshold: *threshold,
            };
        }
    }
    match scanner {
        Scanner::Initial { .. } => unreachable!(),
        Scanner::Basic { basic, .. } => basic.next(),
        Scanner::Vbase { vbase, .. } => vbase.next(),
        Scanner::Empty { threshold: _ } => None,
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
        Scanner::Empty { threshold: _ } => {}
    }
}
