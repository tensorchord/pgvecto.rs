use super::memory_svecf32::SVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::prelude::*;
use base::scalar::F32;
use base::vector::{SVecf32Borrowed, VectorBorrowed};
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, _typmod: i32) -> SVecf32Output {
    fn solve<T>(option: Option<T>, hint: &str) -> T {
        if let Some(x) = option {
            x
        } else {
            bad_literal(hint);
        }
    }
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum State {
        MatchingLeft,
        Reading,
        MatchedRight,
    }
    use State::*;
    let input = input.to_bytes();
    let mut indexes = Vec::<u16>::new();
    let mut values = Vec::<F32>::new();
    let mut state = MatchingLeft;
    let mut token: Option<String> = None;
    let mut index = 0;
    for &c in input {
        match (state, c) {
            (MatchingLeft, b'[') => {
                state = Reading;
            }
            (Reading, b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-') => {
                let token = token.get_or_insert(String::new());
                token.push(char::from_u32(c as u32).unwrap());
            }
            (Reading, b',') => {
                let token = solve(token.take(), "Expect a number.");
                let value: F32 = solve(token.parse().ok(), "Bad number.");
                if !value.is_zero() {
                    indexes.push(index);
                    values.push(value);
                }
                index = match index.checked_add(1) {
                    Some(x) => x,
                    None => check_value_dims(65536).get(),
                };
            }
            (Reading, b']') => {
                if let Some(token) = token.take() {
                    let value: F32 = solve(token.parse().ok(), "Bad number.");
                    if !value.is_zero() {
                        indexes.push(index);
                        values.push(value);
                    }
                    index = match index.checked_add(1) {
                        Some(x) => x,
                        None => check_value_dims(65536).get(),
                    };
                }
                state = MatchedRight;
            }
            (_, b' ') => {}
            _ => {
                bad_literal(&format!("Bad character with ascii {:#x}.", c));
            }
        }
    }
    if state != MatchedRight {
        bad_literal("Bad sequence");
    }
    SVecf32Output::new(SVecf32Borrowed::new(
        check_value_dims(index as usize).get(),
        &indexes,
        &values,
    ))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let vec = vector.for_borrow().to_vec();
    let mut iter = vec.iter();
    if let Some(x) = iter.next() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for x in iter {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
