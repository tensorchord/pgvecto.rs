use std::collections::HashMap;

use num_traits::Zero;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ParseVectorError {
    #[error("The input string is empty.")]
    EmptyString {},
    #[error("Bad character at position {position}")]
    BadCharacter { position: usize },
    #[error("Bad parentheses character '{character}'")]
    BadParentheses { character: char },
    #[error("Too long number at position {position}")]
    TooLongNumber { position: usize },
    #[error("Too short number at position {position}")]
    TooShortNumber { position: usize },
    #[error("Bad parsing at position {position}")]
    BadParsing { position: usize },
    #[error("Index out of bounds: the dim is {dims} but the index is {index}")]
    OutOfBound { dims: usize, index: usize },
    #[error("The dimension should be {min} < dim < {max}, but it is actually {dims}")]
    InvalidDimension { dims: usize, min: usize, max: usize },
    #[error("Indexes need to be unique, but there are more than one same index {index}")]
    IndexConflict { index: usize },
}

#[inline(always)]
pub fn parse_vector<T, F>(input: &[u8], reserve: usize, f: F) -> Result<Vec<T>, ParseVectorError>
where
    F: Fn(&str) -> Option<T>,
{
    use arrayvec::ArrayVec;
    if input.is_empty() {
        return Err(ParseVectorError::EmptyString {});
    }
    let left = 'a: {
        for position in 0..input.len() - 1 {
            match input[position] {
                b'[' => break 'a position,
                b' ' => continue,
                _ => return Err(ParseVectorError::BadCharacter { position }),
            }
        }
        return Err(ParseVectorError::BadParentheses { character: '[' });
    };
    let right = 'a: {
        for position in (1..input.len()).rev() {
            match input[position] {
                b']' => break 'a position,
                b' ' => continue,
                _ => return Err(ParseVectorError::BadCharacter { position }),
            }
        }
        return Err(ParseVectorError::BadParentheses { character: ']' });
    };
    let mut vector = Vec::<T>::with_capacity(reserve);
    let mut token: ArrayVec<u8, 48> = ArrayVec::new();
    for position in left + 1..right {
        let c = input[position];
        match c {
            b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-' => {
                if token.is_empty() {
                    token.push(b'$');
                }
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
            }
            b',' => {
                if !token.is_empty() {
                    // Safety: all bytes in `token` are ascii characters
                    let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                    let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                    vector.push(num);
                    token.clear();
                } else {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
            }
            b' ' => (),
            _ => return Err(ParseVectorError::BadCharacter { position }),
        }
    }
    if !token.is_empty() {
        let position = right;
        // Safety: all bytes in `token` are ascii characters
        let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
        let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
        vector.push(num);
        token.clear();
    }
    Ok(vector)
}

#[derive(PartialEq)]
enum ParseState {
    Start,
    LeftBracket,
    Index,
    Value,
    Splitter,
    Comma,
    Length,
}

#[inline(always)]
pub fn parse_pgvector_svector<T: Zero + Clone, F>(
    input: &[u8],
    f: F,
) -> Result<(Vec<u32>, Vec<T>, usize), ParseVectorError>
where
    F: Fn(&str) -> Option<T>,
{
    use arrayvec::ArrayVec;
    if input.is_empty() {
        return Err(ParseVectorError::EmptyString {});
    }
    let mut token: ArrayVec<u8, 48> = ArrayVec::new();

    let mut indexes = Vec::<u32>::new();
    let mut values = Vec::<T>::new();
    let mut all_indexes = Vec::<u32>::new();
    let mut index: u32 = u32::MAX;

    let mut state = ParseState::Start;
    let mut position = 0;
    loop {
        if position >= input.len() {
            break;
        }
        match state {
            ParseState::Start => {
                let c = input[position];
                match c {
                    b'{' => {
                        state = ParseState::LeftBracket;
                    }
                    b' ' => {}
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
            ParseState::LeftBracket => {
                let c = input[position];
                match c {
                    b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-' => {
                        // Do not read it here, goto Index to read
                        position -= 1;
                        state = ParseState::Index;
                    }
                    b'}' => {
                        state = ParseState::Splitter;
                    }
                    b' ' => {}
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
            ParseState::Index => {
                let c = input[position];
                match c {
                    b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-' => {
                        if token.is_empty() {
                            token.push(b'$');
                        }
                        if token.try_push(c).is_err() {
                            return Err(ParseVectorError::TooLongNumber { position });
                        }
                    }
                    b':' => {
                        if token.is_empty() {
                            return Err(ParseVectorError::TooShortNumber { position });
                        }
                        let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                        index = s
                            .parse::<u32>()
                            .map_err(|_| ParseVectorError::BadParsing { position })?;
                        token.clear();
                        state = ParseState::Value;
                    }
                    b' ' => {}
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
            ParseState::Value => {
                let c = input[position];
                match c {
                    b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-' => {
                        if token.is_empty() {
                            token.push(b'$');
                        }
                        if token.try_push(c).is_err() {
                            return Err(ParseVectorError::TooLongNumber { position });
                        }
                    }
                    b',' => {
                        if token.is_empty() {
                            return Err(ParseVectorError::TooShortNumber { position });
                        }
                        let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                        let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                        if !num.is_zero() {
                            indexes.push(index);
                            values.push(num);
                        }
                        all_indexes.push(index);
                        token.clear();
                        state = ParseState::Comma;
                    }
                    // Bracket ended with number
                    b'}' => {
                        if token.is_empty() {
                            return Err(ParseVectorError::TooShortNumber { position });
                        }
                        let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                        let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                        if !num.is_zero() {
                            indexes.push(index);
                            values.push(num);
                        }
                        all_indexes.push(index);
                        token.clear();
                        state = ParseState::Splitter;
                    }
                    b' ' => {}
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
            ParseState::Comma => {
                let c = input[position];
                match c {
                    b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-' => {
                        // Do not read it here, goto Index to read
                        position -= 1;
                        state = ParseState::Index;
                    }
                    b'}' => {
                        // Bracket ended with comma
                        state = ParseState::Splitter;
                    }
                    b' ' => {}
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
            ParseState::Splitter => {
                let c = input[position];
                match c {
                    b'/' => {
                        state = ParseState::Length;
                    }
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
            ParseState::Length => {
                let c = input[position];
                match c {
                    b'0'..=b'9' => {
                        if token.is_empty() {
                            token.push(b'$');
                        }
                        if token.try_push(c).is_err() {
                            return Err(ParseVectorError::TooLongNumber { position });
                        }
                    }
                    _ => return Err(ParseVectorError::BadCharacter { position }),
                }
            }
        }
        position += 1;
    }
    if state != ParseState::Length {
        return Err(ParseVectorError::BadParsing {
            position: input.len(),
        });
    }
    if token.is_empty() {
        return Err(ParseVectorError::TooShortNumber { position });
    }
    let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
    let dims = s
        .parse::<usize>()
        .map_err(|_| ParseVectorError::BadParsing { position })?;

    // Check dimension out of bound
    if dims == 0 || dims >= 1048576 {
        return Err(ParseVectorError::InvalidDimension {
            dims,
            min: 0,
            max: 1048576,
        });
    }
    // Check index out of bound
    for index in all_indexes.clone() {
        if index as usize >= dims {
            return Err(ParseVectorError::OutOfBound {
                dims,
                index: index as usize,
            });
        }
    }
    // Check index conflicts
    let mut result: HashMap<u32, usize> = HashMap::new();
    for index in all_indexes {
        if let Some(value) = result.get(&index) {
            if *value == 1 {
                return Err(ParseVectorError::IndexConflict {
                    index: index as usize,
                });
            }
        }
        *result.entry(index).or_insert(0) += 1;
    }

    let mut indices = (0..indexes.len()).collect::<Vec<_>>();
    indices.sort_by_key(|&i| &indexes[i]);
    let sorted_values: Vec<T> = indices
        .iter()
        .map(|i| values.get(*i).unwrap().clone())
        .collect();
    indexes.sort();
    Ok((indexes, sorted_values, dims))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use base::scalar::F32;

    use super::*;

    #[test]
    fn test_svector_parse_accept() {
        let exprs: HashMap<&str, (Vec<u32>, Vec<F32>, usize)> = HashMap::from([
            ("{}/1", (vec![], vec![], 1)),
            ("{0:1}/1", (vec![0], vec![F32(1.0)], 1)),
            (
                "{0:1, 1:-2, }/2",
                (vec![0, 1], vec![F32(1.0), F32(-2.0)], 2),
            ),
            ("{0:1, 1:1.5}/2", (vec![0, 1], vec![F32(1.0), F32(1.5)], 2)),
            (
                "{0:+3, 2:-4.1}/3",
                (vec![0, 2], vec![F32(3.0), F32(-4.1)], 3),
            ),
            ("{0:0, 1:0, 2:0}/3", (vec![], vec![], 3)),
            (
                "{3:3, 2:2, 1:1, 0:0}/4",
                (vec![1, 2, 3], vec![F32(1.0), F32(2.0), F32(3.0)], 4),
            ),
        ]);
        for (e, ans) in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_ok(), "at expr {:?}: {:?}", e, ret);
            assert_eq!(ret.unwrap(), ans, "at expr {:?}", e);
        }
    }

    #[test]
    fn test_svector_parse_reject() {
        let exprs: HashMap<&str, ParseVectorError> = HashMap::from([
            ("{", ParseVectorError::BadParsing { position: 1 }),
            ("}", ParseVectorError::BadCharacter { position: 0 }),
            ("{:", ParseVectorError::BadCharacter { position: 1 }),
            (":}", ParseVectorError::BadCharacter { position: 0 }),
            (
                "{}/0",
                ParseVectorError::InvalidDimension {
                    dims: 0,
                    min: 0,
                    max: 1048576,
                },
            ),
            (
                "{}/1919810",
                ParseVectorError::InvalidDimension {
                    dims: 1919810,
                    min: 0,
                    max: 1048576,
                },
            ),
            ("{0:1, 0:2}/1", ParseVectorError::IndexConflict { index: 0 }),
            (
                "{0:1, 1:1.5}/1",
                ParseVectorError::OutOfBound { dims: 1, index: 1 },
            ),
            (
                "{0:0, 1:0, 2:0}/2",
                ParseVectorError::OutOfBound { dims: 2, index: 2 },
            ),
            (
                "{2:0, 1:0}/2",
                ParseVectorError::OutOfBound { dims: 2, index: 2 },
            ),
            (
                "{2:0, 1:0, }/2",
                ParseVectorError::OutOfBound { dims: 2, index: 2 },
            ),
            (
                "{0:1, 1:2, 2:3}",
                ParseVectorError::BadParsing { position: 15 },
            ),
            (
                "{0:1, 1:2, 2:3",
                ParseVectorError::BadParsing { position: 14 },
            ),
            (
                "{0:1, 1:2}/",
                ParseVectorError::TooShortNumber { position: 11 },
            ),
            ("{0}/5", ParseVectorError::BadCharacter { position: 2 }),
            ("{0:}/5", ParseVectorError::TooShortNumber { position: 3 }),
            ("{:0}/5", ParseVectorError::BadCharacter { position: 1 }),
            (
                "{0:, 1:2}/5",
                ParseVectorError::TooShortNumber { position: 3 },
            ),
            ("{0:1, 1}/5", ParseVectorError::BadCharacter { position: 7 }),
            ("/2", ParseVectorError::BadCharacter { position: 0 }),
            ("{}/1/2", ParseVectorError::BadCharacter { position: 4 }),
            (
                "{0:1, 1:2}/4/2",
                ParseVectorError::BadCharacter { position: 12 },
            ),
            ("{}/-4", ParseVectorError::BadCharacter { position: 3 }),
            (
                "{1,2,3,4}/5",
                ParseVectorError::BadCharacter { position: 2 },
            ),
        ]);
        for (e, err) in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_err(), "at expr {:?}: {:?}", e, ret);
            assert_eq!(ret.unwrap_err(), err, "at expr {:?}", e);
        }
    }
}
