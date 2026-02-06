use common::error::DbError;

pub const INT_SIZE: usize = 4;
pub const BIGINT_SIZE: usize = 8;

pub enum Col {
    Int(i32),
    BigInt(i64),
    Varchar(String, usize),
    Link,
}

impl Col {
    pub fn int(value: i32) -> Self {
        Self::Int(value)
    }

    pub fn big_int(value: i64) -> Self {
        Self::BigInt(value)
    }

    pub fn varchar(value: String, size: usize) -> Self {
        assert!(value.len() < size);
        Self::Varchar(value, size)
    }

    pub fn parse_int(buffer: &mut [u8]) -> Result<Self, DbError> {
        let mut value = [0u8; INT_SIZE];
        value.copy_from_slice(buffer);
        let value = i32::from_be_bytes(value);
        Ok(Self::Int(value))
    }

    pub fn parse_bigint(buffer: &mut [u8]) -> Result<Self, DbError> {
        let mut value = [0u8; BIGINT_SIZE];
        value.copy_from_slice(buffer);
        let value = i64::from_be_bytes(value);
        Ok(Self::BigInt(value))
    }

    pub fn parse_varchar(buffer: &mut [u8], size: usize) -> Result<Self, DbError> {
        let mut value = vec![0u8; size];
        value.copy_from_slice(buffer);
        let value = String::from_utf8_lossy(&value).to_string();
        Ok(Self::Varchar(value, size))
    }
}

impl TryInto<Vec<u8>> for Col {
    type Error = DbError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::Int(value) => {
                let mut buffer = vec![0u8; INT_SIZE];
                buffer.copy_from_slice(&value.to_be_bytes());
                Ok(buffer)
            }
            Self::BigInt(value) => {
                let mut buffer = vec![0u8; BIGINT_SIZE];
                buffer.copy_from_slice(&value.to_be_bytes());
                Ok(buffer)
            }
            Self::Varchar(value, size) => {
                let mut buffer = vec![0u8; size];
                buffer.copy_from_slice(value.as_bytes());
                Ok(buffer)
            }
            Self::Link => {
                todo!()
            }
        }
    }
}
