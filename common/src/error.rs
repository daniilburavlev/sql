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
}

impl DbError {
    pub fn unexpected(err: &str) -> Self {
        DbError::Unexpected(err.to_string())
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
}
