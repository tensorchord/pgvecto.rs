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

#[derive(PartialEq, Debug, Clone)]
enum ParseState {
    Start,
    LeftBracket,
    Index,
    Colon,
    Value,
    Comma,
    RightBracket,
    Splitter,
    Dims,
}

#[inline(always)]
pub fn svector_sorted<T: Zero + Clone + PartialEq>(
    indexes: &[u32],
    values: &[T],
) -> (Vec<u32>, Vec<T>) {
    let mut indices = (0..indexes.len()).collect::<Vec<_>>();
    indices.sort_by_key(|&i| &indexes[i]);

    let mut sorted_indexes: Vec<u32> = Vec::with_capacity(indexes.len());
    let mut sorted_values: Vec<T> = Vec::with_capacity(indexes.len());
    for i in indices {
        sorted_indexes.push(*indexes.get(i).unwrap());
        sorted_values.push(values.get(i).unwrap().clone());
    }
    (sorted_indexes, sorted_values)
}

#[inline(always)]
pub fn svector_filter_nonzero<T: Zero + Clone + PartialEq>(
    indexes: &mut Vec<u32>,
    values: &mut Vec<T>,
) {
    // Index must be sorted!
    let mut i = 0;
    let mut j = 0;
    while j < values.len() {
        if !values[j].is_zero() {
            indexes[i] = indexes[j];
            values[i] = values[j].clone();
            i += 1;
        }
        j += 1;
    }
    indexes.truncate(i);
    values.truncate(i);
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

    let mut state = ParseState::Start;
    for (position, c) in input.iter().copied().enumerate() {
        state = match (&state, c) {
            (_, b' ') => state,
            (ParseState::Start, b'{') => ParseState::LeftBracket,
            (
                ParseState::LeftBracket | ParseState::Index | ParseState::Comma,
                b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-',
            ) => {
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
                ParseState::Index
            }
            (ParseState::Colon, b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-') => {
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
                ParseState::Value
            }
            (ParseState::LeftBracket | ParseState::Comma, b'}') => ParseState::RightBracket,
            (ParseState::Index, b':') => {
                let s = unsafe { std::str::from_utf8_unchecked(&token[..]) };
                let index = s
                    .parse::<u32>()
                    .map_err(|_| ParseVectorError::BadParsing { position })?;
                indexes.push(index);
                token.clear();
                ParseState::Colon
            }
            (ParseState::Value, b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-') => {
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
                ParseState::Value
            }
            (ParseState::Value, b',') => {
                let s = unsafe { std::str::from_utf8_unchecked(&token[..]) };
                let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                values.push(num);
                token.clear();
                ParseState::Comma
            }
            (ParseState::Value, b'}') => {
                if token.is_empty() {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
                let s = unsafe { std::str::from_utf8_unchecked(&token[..]) };
                let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                values.push(num);
                token.clear();
                ParseState::RightBracket
            }
            (ParseState::RightBracket, b'/') => ParseState::Splitter,
            (ParseState::Dims | ParseState::Splitter, b'0'..=b'9') => {
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
                ParseState::Dims
            }
            (_, _) => {
                return Err(ParseVectorError::BadCharacter { position });
            }
        }
    }
    if state != ParseState::Dims {
        return Err(ParseVectorError::BadParsing {
            position: input.len(),
        });
    }
    let s = unsafe { std::str::from_utf8_unchecked(&token[..]) };
    let dims = s
        .parse::<usize>()
        .map_err(|_| ParseVectorError::BadParsing {
            position: input.len(),
        })?;
    Ok((indexes, values, dims))
}

#[cfg(test)]
mod tests {
    use base::scalar::F32;

    use super::*;

    #[test]
    fn test_svector_parse_accept() {
        let exprs: Vec<(&str, (Vec<u32>, Vec<F32>, usize))> = vec![
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
            (
                "{0:0, 1:0, 2:0}/3",
                (vec![0, 1, 2], vec![F32(0.0), F32(0.0), F32(0.0)], 3),
            ),
            (
                "{3:3, 2:2, 1:1, 0:0}/4",
                (
                    vec![3, 2, 1, 0],
                    vec![F32(3.0), F32(2.0), F32(1.0), F32(0.0)],
                    4,
                ),
            ),
        ];
        for (e, parsed) in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_ok(), "at expr {:?}: {:?}", e, ret);
            assert_eq!(ret.unwrap(), parsed, "parsed at expr {:?}", e);
        }
    }

    #[test]
    fn test_svector_parse_reject() {
        let exprs: Vec<(&str, ParseVectorError)> = vec![
            ("{", ParseVectorError::BadParsing { position: 1 }),
            ("}", ParseVectorError::BadCharacter { position: 0 }),
            ("{:", ParseVectorError::BadCharacter { position: 1 }),
            (":}", ParseVectorError::BadCharacter { position: 0 }),
            (
                "{0:1, 1:2, 2:3}",
                ParseVectorError::BadParsing { position: 15 },
            ),
            (
                "{0:1, 1:2, 2:3",
                ParseVectorError::BadParsing { position: 14 },
            ),
            ("{0:1, 1:2}/", ParseVectorError::BadParsing { position: 11 }),
            ("{0}/5", ParseVectorError::BadCharacter { position: 2 }),
            ("{0:}/5", ParseVectorError::BadCharacter { position: 3 }),
            ("{:0}/5", ParseVectorError::BadCharacter { position: 1 }),
            (
                "{0:, 1:2}/5",
                ParseVectorError::BadCharacter { position: 3 },
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
        ];
        for (e, err) in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_err(), "at expr {:?}: {:?}", e, ret);
            assert_eq!(ret.unwrap_err(), err, "parsed at expr {:?}", e);
        }
    }

    #[test]
    fn test_svector_parse_filter() {
        let exprs: Vec<(&str, (Vec<u32>, Vec<F32>, usize), (Vec<u32>, Vec<F32>))> = vec![
            ("{}/0", (vec![], vec![], 0), (vec![], vec![])),
            ("{}/1919810", (vec![], vec![], 1919810), (vec![], vec![])),
            (
                "{0:1, 0:2}/1",
                (vec![0, 0], vec![F32(1.0), F32(2.0)], 1),
                (vec![0, 0], vec![F32(1.0), F32(2.0)]),
            ),
            (
                "{0:1, 1:1.5}/1",
                (vec![0, 1], vec![F32(1.0), F32(1.5)], 1),
                (vec![0, 1], vec![F32(1.0), F32(1.5)]),
            ),
            (
                "{0:0, 1:0, 2:0}/2",
                (vec![0, 1, 2], vec![F32(0.0), F32(0.0), F32(0.0)], 2),
                (vec![], vec![]),
            ),
            (
                "{2:0, 1:0}/2",
                (vec![2, 1], vec![F32(0.0), F32(0.0)], 2),
                (vec![], vec![]),
            ),
            (
                "{2:0, 1:0, }/2",
                (vec![2, 1], vec![F32(0.0), F32(0.0)], 2),
                (vec![], vec![]),
            ),
            (
                "{3:2, 2:1, 1:0, 0:-1}/4",
                (
                    vec![3, 2, 1, 0],
                    vec![F32(2.0), F32(1.0), F32(0.0), F32(-1.0)],
                    4,
                ),
                (vec![0, 2, 3], vec![F32(-1.0), F32(1.0), F32(2.0)]),
            ),
        ];
        for (e, parsed, filtered) in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_ok(), "at expr {:?}: {:?}", e, ret);
            assert_eq!(ret.unwrap(), parsed, "parsed at expr {:?}", e);

            let (indexes, values, _) = parsed;
            let (mut indexes, mut values) = svector_sorted(&indexes, &values);
            svector_filter_nonzero(&mut indexes, &mut values);
            assert_eq!((indexes, values), filtered, "filtered at expr {:?}", e);
        }
    }
}
