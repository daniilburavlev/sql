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
}

impl DbError {
    pub fn unexpected(err: &str) -> Self {
        DbError::Unexpected(err.to_string())
    }

    pub fn eof(err: &str) -> Self {
        DbError::EOF(err.to_string())
    }

    pub fn invalid_input(err: &str) -> Self {
        DbError::InvalidInput(err.to_string())
    }
}

impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> Self {
        DbError::IO(err.to_string())
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
}
