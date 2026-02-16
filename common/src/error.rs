use std::num::ParseIntError;

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum DbError {
    #[error("IO ERR: {0}")]
    IO(String),
    #[error("ERR: {0}")]
    Unexpected(String),
    #[error("encoding exception")]
    Encoding,
    #[error("max size error, received: {0}, limit: {1}")]
    MaxSize(usize, usize),
    #[error("unexpected EOF: {0}")]
    EOF(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("field '{0}' of relation '{1}' doesn't exist")]
    FieldNotFound(String, String),
    #[error("PRIMARY_KEY constraint is not set")]
    PrimaryKeyNotSet,
}

impl DbError {
    pub fn unexpected(err: &str) -> Self {
        Self::Unexpected(err.to_string())
    }

    pub fn eof(err: &str) -> Self {
        Self::EOF(err.to_string())
    }

    pub fn invalid_input(err: &str) -> Self {
        Self::InvalidInput(err.to_string())
    }

    pub fn field_not_found(field: &str, relation: &str) -> Self {
        Self::FieldNotFound(field.to_string(), relation.to_string())
    }
}

impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> Self {
        DbError::IO(err.to_string())
    }
}

impl From<ParseIntError> for DbError {
    fn from(err: ParseIntError) -> Self {
        DbError::InvalidInput(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_to_error() {
        let msg = "test";
        let error = std::io::Error::other(msg);
        let error: DbError = error.into();
        assert_eq!(DbError::IO(msg.to_string()), error);
    }

    #[test]
    fn unexpected_error() {
        let msg = "err";
        assert_eq!(
            DbError::unexpected(msg),
            DbError::Unexpected(msg.to_string())
        );
    }

    #[test]
    fn eof_error() {
        let msg = "err";
        assert_eq!(DbError::eof(msg), DbError::EOF(msg.to_string()));
    }

    #[test]
    fn invalid_intput_error() {
        let msg = "err";
        assert_eq!(
            DbError::invalid_input(msg),
            DbError::InvalidInput(msg.to_string())
        );
    }

    #[test]
    fn field_not_found() {
        let field = "win";
        let table = "casino";
        assert_eq!(
            DbError::field_not_found(field, table),
            DbError::FieldNotFound(field.to_string(), table.to_string())
        );
    }

    #[test]
    #[should_panic]
    fn from_parse_int_error() {
        fn parse_int() -> Result<(), DbError> {
            let _: i32 = "test".parse()?;
            Ok(())
        }
        parse_int().unwrap();
    }
}
