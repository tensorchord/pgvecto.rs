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

#[derive(PartialEq, Debug)]
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
pub fn svector_filter_nonzero<T: Zero + Clone + PartialEq>(
    indexes: &[u32],
    values: &[T],
) -> (Vec<u32>, Vec<T>) {
    let non_zero_indexes: Vec<u32> = indexes
        .iter()
        .enumerate()
        .filter(|(i, _)| values.get(*i).unwrap() != &T::zero())
        .map(|(_, x)| *x)
        .collect();
    let non_zero_values: Vec<T> = indexes
        .iter()
        .enumerate()
        .filter(|(i, _)| values.get(*i).unwrap() != &T::zero())
        .map(|(i, _)| values.get(i).unwrap().clone())
        .collect();
    (non_zero_indexes, non_zero_values)
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
    for (position, char) in input.iter().enumerate() {
        let c = *char;
        match (&state, c) {
            (_, b' ') => {}
            (ParseState::Start, b'{') => {
                state = ParseState::LeftBracket;
            }
            (
                ParseState::LeftBracket | ParseState::Index | ParseState::Comma,
                b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-',
            ) => {
                if token.is_empty() {
                    token.push(b'$');
                }
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
                state = ParseState::Index;
            }
            (ParseState::LeftBracket | ParseState::Comma, b'}') => {
                state = ParseState::Splitter;
            }
            (ParseState::Index, b':') => {
                if token.is_empty() {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
                let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                let index = s
                    .parse::<u32>()
                    .map_err(|_| ParseVectorError::BadParsing { position })?;
                indexes.push(index);
                token.clear();
                state = ParseState::Value;
            }
            (ParseState::Value, b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-') => {
                if token.is_empty() {
                    token.push(b'$');
                }
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
            }
            (ParseState::Value, b',') => {
                if token.is_empty() {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
                let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                values.push(num);
                token.clear();
                state = ParseState::Comma;
            }
            (ParseState::Value, b'}') => {
                if token.is_empty() {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
                let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
                let num = f(s).ok_or(ParseVectorError::BadParsing { position })?;
                values.push(num);
                token.clear();
                state = ParseState::Splitter;
            }
            (ParseState::Splitter, b'/') => {
                state = ParseState::Length;
            }
            (ParseState::Length, b'0'..=b'9') => {
                if token.is_empty() {
                    token.push(b'$');
                }
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
            }
            (_, _) => {
                return Err(ParseVectorError::BadCharacter { position });
            }
        }
    }
    if state != ParseState::Length {
        return Err(ParseVectorError::BadParsing {
            position: input.len(),
        });
    }
    if token.is_empty() {
        return Err(ParseVectorError::TooShortNumber {
            position: input.len(),
        });
    }
    let s = unsafe { std::str::from_utf8_unchecked(&token[1..]) };
    let dims = s
        .parse::<usize>()
        .map_err(|_| ParseVectorError::BadParsing {
            position: input.len(),
        })?;

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
                    vec![0, 1, 2, 3],
                    vec![F32(0.0), F32(1.0), F32(2.0), F32(3.0)],
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
                (vec![1, 2], vec![F32(0.0), F32(0.0)], 2),
                (vec![], vec![]),
            ),
            (
                "{2:0, 1:0, }/2",
                (vec![1, 2], vec![F32(0.0), F32(0.0)], 2),
                (vec![], vec![]),
            ),
        ];
        for (e, parsed, filtered) in exprs {
            let ret = parse_pgvector_svector(e.as_bytes(), |s| s.parse::<F32>().ok());
            assert!(ret.is_ok(), "at expr {:?}: {:?}", e, ret);
            assert_eq!(ret.unwrap(), parsed, "parsed at expr {:?}", e);

            let (indexes, values, _) = parsed;
            let nonzero = svector_filter_nonzero(&indexes, &values);
            assert_eq!(nonzero, filtered, "filtered at expr {:?}", e);
        }
    }
}
