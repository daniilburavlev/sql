use common::{Pageable, error::DbError, read_num};

pub const INT_SIZE: usize = 4;
pub const BIGINT_SIZE: usize = 8;

pub const COL_TYPE_SIZE: usize = 1;
pub const VARCHAR_LEN_SIZE: usize = 2;

pub const INT_TYPE: u8 = 1;
pub const BIG_INT_TYPE: u8 = 2;
pub const VARCHAR_TYPE: u8 = 3;

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum Col {
    Int(i32),
    BigInt(i64),
    Varchar(String, u16),
}

impl Col {
    pub fn get_type(&self) -> u8 {
        match self {
            Self::Int(_) => INT_TYPE,
            Self::BigInt(_) => BIG_INT_TYPE,
            Self::Varchar(_, _) => VARCHAR_TYPE,
        }
    }
    pub fn int(value: i32) -> Self {
        Self::Int(value)
    }

    pub fn big_int(value: i64) -> Self {
        Self::BigInt(value)
    }

    pub fn varchar(value: &str, size: u16) -> Self {
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

    pub fn parse_varchar(buffer: &[u8]) -> Result<(Self, usize), DbError> {
        let mut offset = 0;
        let max_len = read_num!(buffer, u16, offset);
        offset += VARCHAR_LEN_SIZE;
        let len = read_num!(buffer, u16, offset);
        offset += VARCHAR_LEN_SIZE;

        let mut value = vec![0u8; len as usize];
        value.copy_from_slice(&buffer[offset..offset + len as usize]);
        let value = String::from_utf8_lossy(&value);
        offset += max_len as usize;
        Ok((Col::Varchar(value.to_string(), max_len), offset))
    }
}

impl Pageable for Col {
    fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        let mut offset = 1;
        buffer[0] = self.get_type();
        match self {
            Self::Int(value) => {
                buffer[offset..offset + INT_SIZE].copy_from_slice(&value.to_be_bytes());
                offset += INT_SIZE;
                Ok(offset)
            }
            Self::BigInt(value) => {
                buffer[offset..offset + BIGINT_SIZE].copy_from_slice(&value.to_be_bytes());
                offset += BIGINT_SIZE;
                Ok(offset)
            }
            Self::Varchar(value, size) => {
                buffer[offset..offset + VARCHAR_LEN_SIZE].copy_from_slice(&(*size).to_be_bytes());
                offset += VARCHAR_LEN_SIZE;
                let len = value.len();
                buffer[offset..offset + VARCHAR_LEN_SIZE]
                    .copy_from_slice(&(len as u16).to_be_bytes());
                offset += VARCHAR_LEN_SIZE;

                buffer[offset..offset + len].copy_from_slice(value.as_bytes());
                Ok(COL_TYPE_SIZE + VARCHAR_LEN_SIZE * 2 + (*size as usize))
            }
        }
    }

    fn read(buffer: &[u8]) -> Result<(Self, usize), DbError> {
        let mut offset = 0;
        let col_type = buffer[offset];
        offset += COL_TYPE_SIZE;

        match col_type {
            INT_TYPE => {
                let value = Col::parse_int(&buffer[offset..offset + INT_SIZE])?;
                offset += INT_SIZE;
                Ok((value, offset))
            }
            BIG_INT_TYPE => {
                let value = Col::parse_bigint(&buffer[offset..offset + BIGINT_SIZE])?;
                offset += BIGINT_SIZE;
                Ok((value, offset))
            }
            VARCHAR_TYPE => {
                let (varchar, read) = Col::parse_varchar(&buffer[offset..])?;
                offset += read;
                Ok((varchar, offset))
            }
            _ => Err(DbError::Encoding),
        }
    }

    fn size(&self) -> usize {
        match self {
            Col::Int(_) => COL_TYPE_SIZE + INT_SIZE,
            Col::BigInt(_) => COL_TYPE_SIZE + BIGINT_SIZE,
            Col::Varchar(_, size) => COL_TYPE_SIZE + VARCHAR_LEN_SIZE * 2 + *size as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_int() {
        let value = 10;
        let int = Col::Int(value);
        let mut buffer = [0u8; COL_TYPE_SIZE + INT_SIZE];
        let size = int.write(&mut buffer).unwrap();
        assert_eq!(5, size);
        let (col, read) = Col::read(&buffer).unwrap();
        assert_eq!(5, read);
        assert_eq!(Col::int(value), col);
    }

    #[test]
    fn write_read_big_int() {
        let value = 10;
        let int = Col::BigInt(value);
        let mut buffer = [0u8; COL_TYPE_SIZE + BIGINT_SIZE];
        let size = int.write(&mut buffer).unwrap();
        assert_eq!(9, size);
        let (col, read) = Col::read(&buffer).unwrap();
        assert_eq!(9, read);
        assert_eq!(Col::big_int(value), col);
    }

    #[test]
    fn write_read_varchar() {
        let value = "Hello";
        let max_size = 256;
        let varchar = Col::Varchar(value.to_string(), max_size);
        let size = COL_TYPE_SIZE + 2 * VARCHAR_LEN_SIZE + (max_size as usize);
        let mut buffer = vec![0u8; size];
        let read = varchar.write(&mut buffer).unwrap();
        assert_eq!(size, read);
        let (col, read) = Col::read(&buffer).unwrap();
        assert_eq!(size, read);
        assert_eq!(Col::Varchar(value.to_string(), max_size), col);
    }

    #[test]
    fn row_size() {
        assert_eq!(COL_TYPE_SIZE + INT_SIZE, Col::Int(1).size());
        assert_eq!(COL_TYPE_SIZE + BIGINT_SIZE, Col::BigInt(1).size());
        let len = 10;
        assert_eq!(
            COL_TYPE_SIZE + 2 * VARCHAR_LEN_SIZE + len,
            Col::Varchar(0.to_string(), len as u16).size()
        );
    }

    #[test]
    fn invalid_col_type() {
        let buffer = [244u8; 1];
        match Col::read(&buffer) {
            Err(DbError::Encoding) => {}
            _ => panic!("expected error"),
        }
    }
}
