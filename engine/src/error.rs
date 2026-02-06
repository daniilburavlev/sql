use thiserror::Error;

#[derive(Error, Debug)]
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
