use common::error::DbError;

pub const INT_SIZE: usize = 4;
pub const BIGINT_SIZE: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq)]
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

    pub fn parse_int(buffer: &[u8]) -> Result<Self, DbError> {
        let mut value = [0u8; INT_SIZE];
        value.copy_from_slice(buffer);
        let value = i32::from_be_bytes(value);
        Ok(Self::Int(value))
    }

    pub fn parse_bigint(buffer: &[u8]) -> Result<Self, DbError> {
        let mut value = [0u8; BIGINT_SIZE];
        value.copy_from_slice(buffer);
        let value = i64::from_be_bytes(value);
        Ok(Self::BigInt(value))
    }

    pub fn parse_varchar(buffer: &[u8], size: usize) -> Result<Self, DbError> {
        let mut len = [0u8; INT_SIZE];
        len.copy_from_slice(&buffer[0..INT_SIZE]);
        let len = u32::from_be_bytes(len);

        let mut value = vec![0u8; len as usize];
        value.copy_from_slice(&buffer[INT_SIZE..INT_SIZE + len as usize]);
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
                let len = value.len();
                let mut buffer = vec![0u8; INT_SIZE + size];
                buffer[0..INT_SIZE].copy_from_slice(&(len as u32).to_be_bytes());

                buffer[INT_SIZE..INT_SIZE + len].copy_from_slice(value.as_bytes());
                Ok(buffer)
            }
            Self::Link => {
                todo!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read() {
        let int = Col::int(10);
        let buffer: Vec<u8> = int.clone().try_into().unwrap();
        let restored = Col::parse_int(&buffer).unwrap();
        assert_eq!(int, restored);

        let big_int = Col::big_int(10);
        let buffer: Vec<u8> = big_int.clone().try_into().unwrap();
        let restored = Col::parse_bigint(&buffer).unwrap();
        assert_eq!(big_int, restored);

        let varchar = Col::varchar("String".to_string(), 256);
        let buffer: Vec<u8> = varchar.clone().try_into().unwrap();
        assert_eq!(buffer.len(), 256 + INT_SIZE);
        let restored = Col::parse_varchar(&buffer, 256).unwrap();
        assert_eq!(varchar, restored);
    }
}
