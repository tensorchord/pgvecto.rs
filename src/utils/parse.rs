use num_traits::Zero;
use thiserror::Error;

#[derive(Debug, Error)]
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
    Number,
    Comma,
    Colon,
    Start,
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
    let mut dims: usize = usize::MAX;
    let left = 'a: {
        for position in 0..input.len() - 1 {
            match input[position] {
                b'{' => break 'a position,
                b' ' => continue,
                _ => return Err(ParseVectorError::BadCharacter { position }),
            }
        }
        return Err(ParseVectorError::BadParentheses { character: '{' });
    };
    let mut token: ArrayVec<u8, 48> = ArrayVec::new();
    let right = 'a: {
        for position in (1..input.len()).rev() {
            match input[position] {
                b'0'..=b'9' => {
                    if token.try_push(input[position]).is_err() {
                        return Err(ParseVectorError::TooLongNumber { position });
                    }
                }
                b'/' => {
                    token.reverse();
                    let s = unsafe { std::str::from_utf8_unchecked(&token[..]) };
                    // two `dims` are found
                    if dims != usize::MAX {
                        return Err(ParseVectorError::BadCharacter { position });
                    }
                    dims = s
                        .parse::<usize>()
                        .map_err(|_| ParseVectorError::BadParsing { position })?;
                }
                b'}' => {
                    token.clear();
                    break 'a position;
                }
                b' ' => continue,
                _ => return Err(ParseVectorError::BadCharacter { position }),
            }
        }
        return Err(ParseVectorError::BadParentheses { character: '}' });
    };
    // `dims` is not found
    if dims == usize::MAX {
        return Err(ParseVectorError::BadCharacter {
            position: input.len(),
        });
    }
    let mut indexes = Vec::<u32>::new();
    let mut values = Vec::<T>::new();
    let mut index: u32 = u32::MAX;
    let mut state = ParseState::Start;

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
                state = ParseState::Number;
            }
            b',' => {
                if state != ParseState::Number {
                    return Err(ParseVectorError::BadCharacter { position });
                }
                if !token.is_empty() {
                    // Safety: all bytes in `token` are ascii characters
                    let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                    let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                    if index as usize >= dims {
                        return Err(ParseVectorError::OutOfBound {
                            dims,
                            index: index as usize,
                        });
                    }
                    if !num.is_zero() {
                        indexes.push(index);
                        values.push(num);
                    }
                    index = u32::MAX;
                    token.clear();
                    state = ParseState::Comma;
                } else {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
            }
            b':' => {
                if !token.is_empty() {
                    // Safety: all bytes in `token` are ascii characters
                    let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                    index = s
                        .parse::<u32>()
                        .map_err(|_| ParseVectorError::BadParsing { position })?;
                    token.clear();
                    state = ParseState::Colon;
                } else {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
            }
            b' ' => (),
            _ => return Err(ParseVectorError::BadCharacter { position }),
        }
    }
    // A valid case is either
    // - empty string: ""
    // - end with number when a index is extracted:"1:2, 3:4"
    if state != ParseState::Start && (state != ParseState::Number || index == u32::MAX) {
        return Err(ParseVectorError::BadCharacter { position: right });
    }
    if !token.is_empty() {
        let position = right;
        // Safety: all bytes in `token` are ascii characters
        let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
        let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
        if index as usize >= dims {
            return Err(ParseVectorError::OutOfBound {
                dims,
                index: index as usize,
            });
        }
        if !num.is_zero() {
            indexes.push(index);
            values.push(num);
        }
        token.clear();
    }
    // sort values and indexes ascend by indexes
    let mut indices = (0..indexes.len()).collect::<Vec<_>>();
    indices.sort_by_key(|&i| &indexes[i]);
    let sortedValues: Vec<T> = indices
        .iter()
        .map(|i| values.get(*i).unwrap().clone())
        .collect();
    indexes.sort();
    Ok((indexes, sortedValues, dims))
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
            assert!(ret.is_ok(), "at expr {e}");
            assert_eq!(ret.unwrap(), ans, "at expr {e}");
        }
    }

    #[test]
    fn test_svector_parse_reject() {
        let exprs: Vec<&str> = vec![
            "{",
            "}",
            "{:",
            ":}",
            "{0:1, 1:1.5}/1",
            "{0:0, 1:0, 2:0}/2",
            "{0:1, 1:2, 2:3}",
            "{0:1, 1:2, 2:3",
            "{0:1, 1:2}/",
            "{0}/5",
            "{0:}/5",
            "{:0}/5",
            "{0:, 1:2}/5",
            "{0:1, 1}/5",
            "/2",
            "{}/1/2",
        ];
        for e in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_err(), "at expr {e}")
        }
    }
}
