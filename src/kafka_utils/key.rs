use std::{fmt, error, string::FromUtf8Error};

#[derive(Debug, PartialEq, Clone)]
pub struct Key {
    pub namespace: String,
    pub subject: String,
}

const KEY_DELIMITER: &str = ".";

#[derive(Debug, PartialEq)]
pub enum ParseKeyError {
    WrongFormat(String),
    ConvertVec(FromUtf8Error),
}

impl fmt::Display for ParseKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseKeyError::WrongFormat(key) => write!(f, "cannot parse key, needs 2 parts separated by {KEY_DELIMITER}: {key}"),
            ParseKeyError::ConvertVec(e) => write!(f, "cannot convert key to String, error: {e}"),
        }
    }
}

impl error::Error for ParseKeyError {}

impl From<FromUtf8Error> for ParseKeyError {
    fn from(error: FromUtf8Error) -> Self {
        ParseKeyError::ConvertVec(error)
    }
}

impl Key {
    pub fn new(namespace: String, subject: String) -> Self {
        Self { namespace, subject }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        return self.to_string().into_bytes();
    }

    pub fn to_string(&self) -> String {
        let string_list: Vec<&str> = vec![&self.namespace, &self.subject];
        return string_list.join(KEY_DELIMITER);
    }

    pub fn parse_key(key: &Vec<u8>) -> Result<Self, ParseKeyError> {
        if key.len() == 0 {
            return Ok(Self { namespace: Default::default(), subject: Default::default() });
        }
        let key_string = String::from_utf8(key.clone())?;
        let key_list: Vec<_> = key_string.split(KEY_DELIMITER).collect();
        if key_list.len() != 2 {
            return Err(ParseKeyError::WrongFormat(key_string));
        }
        Ok(Self { namespace: key_list[0].to_string(), subject: key_list[1].to_string() })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_new_key() {
        let key = Key::new("ethereum".to_string(), "Transaction".to_string());
        let expected = Key { namespace: "ethereum".to_string(), subject: "Transaction".to_string() };
        assert_eq!(key, expected);
    }

    #[test]
    fn convert_key_to_bytes() {
        let key = Key::new("ethereum".to_string(), "Transaction".to_string());
        let expected: Vec<u8> = vec![101, 116, 104, 101, 114, 101, 117, 109, 46, 84, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110];
        assert_eq!(key.to_bytes(), expected);
    }

    #[test]
    fn convert_key_to_string() {
        let key = Key::new("ethereum".to_string(), "Transaction".to_string());
        let expected: String = String::from("ethereum.Transaction");
        assert_eq!(key.to_string(), expected);
    }

    #[test]
    fn parse_key_with_bytes() {
        let key: Vec<u8> = vec![101, 116, 104, 101, 114, 101, 117, 109, 46, 84, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110];
        let expected = Key { namespace: "ethereum".to_string(), subject: "Transaction".to_string() };
        assert_eq!(Key::parse_key(&key).unwrap(), expected);
    }

    #[test]
    fn parse_key_error() {
        let error_key = vec![0, 159];
        let expected_error = String::from_utf8(error_key.clone()).unwrap_err();
        assert_eq!(Key::parse_key(&error_key).unwrap_err(), ParseKeyError::ConvertVec(expected_error), "convert vec to String error");

        let error_key: Vec<u8> = vec![101, 116, 104, 101, 114, 101, 117, 109, 84, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110];
        assert_eq!(Key::parse_key(&error_key).unwrap_err(), ParseKeyError::WrongFormat("ethereumTransaction".to_string()), "wrong String format error");
    }
}
