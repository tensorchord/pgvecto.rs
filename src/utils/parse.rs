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
