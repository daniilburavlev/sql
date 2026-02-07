use common::error::DbError;

pub const INT_SIZE: usize = 4;
pub const BIGINT_SIZE: usize = 8;

pub const COL_TYPE_SIZE: usize = 1;
pub const VARCHAR_LEN_SIZE: usize = 2;

pub const INT_TYPE: u8 = 1;
pub const BIG_INT_TYPE: u8 = 2;
pub const VARCHAR_TYPE: u8 = 3;
pub const LINK_TYPE: u8 = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Col {
    Int(i32),
    BigInt(i64),
    Varchar(String, usize),
    Link,
}

impl Col {
    pub fn get_type(&self) -> u8 {
        match self {
            Self::Int(_) => INT_TYPE,
            Self::BigInt(_) => BIG_INT_TYPE,
            Self::Varchar(_, _) => VARCHAR_TYPE,
            Self::Link => LINK_TYPE,
        }
    }
    pub fn int(value: i32) -> Self {
        Self::Int(value)
    }

    pub fn big_int(value: i64) -> Self {
        Self::BigInt(value)
    }

    pub fn varchar(value: &str, size: usize) -> Self {
        assert!(value.len() < size);
        Self::Varchar(value.to_string(), size)
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
        let mut len = [0u8; VARCHAR_LEN_SIZE];
        len.copy_from_slice(&buffer[0..VARCHAR_LEN_SIZE]);
        let len = u16::from_be_bytes(len);

        let mut value = vec![0u8; len as usize];
        value.copy_from_slice(&buffer[VARCHAR_LEN_SIZE..VARCHAR_LEN_SIZE + len as usize]);
        let value = String::from_utf8_lossy(&value).to_string();
        Ok(Self::Varchar(value, size))
    }

    pub fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        match self {
            Self::Int(value) => {
                buffer[..INT_SIZE].copy_from_slice(&value.to_be_bytes());
                Ok(INT_SIZE)
            }
            Self::BigInt(value) => {
                buffer[..BIGINT_SIZE].copy_from_slice(&value.to_be_bytes());
                Ok(BIGINT_SIZE)
            }
            Self::Varchar(value, size) => {
                let len = value.len();
                buffer[..VARCHAR_LEN_SIZE].copy_from_slice(&(len as u16).to_be_bytes());

                buffer[VARCHAR_LEN_SIZE..VARCHAR_LEN_SIZE + len].copy_from_slice(value.as_bytes());
                Ok(VARCHAR_LEN_SIZE + size)
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
        let mut buffer = [0u8; INT_SIZE];
        let size = int.write(&mut buffer).unwrap();
        assert_eq!(INT_SIZE, size);
        let restored = Col::parse_int(&buffer).unwrap();
        assert_eq!(int, restored);

        let big_int = Col::big_int(10);
        let mut buffer = [0u8; BIGINT_SIZE];
        let size = big_int.write(&mut buffer).unwrap();
        assert_eq!(BIGINT_SIZE, size);
        let restored = Col::parse_bigint(&buffer).unwrap();
        assert_eq!(big_int, restored);

        let varchar = Col::varchar("String", 256);
        let mut buffer = [0u8; VARCHAR_LEN_SIZE + 256];
        let size = varchar.write(&mut buffer).unwrap();
        assert_eq!(VARCHAR_LEN_SIZE + 256, size);
        assert_eq!(buffer.len(), 256 + VARCHAR_LEN_SIZE);
        let restored = Col::parse_varchar(&buffer, 256).unwrap();
        assert_eq!(varchar, restored);
    }
}
