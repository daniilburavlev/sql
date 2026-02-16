use core::fmt;

use common::{Pageable, error::DbError, read_num};

use crate::col::{BIG_INT_TYPE, INT_TYPE, VARCHAR_LEN_SIZE, VARCHAR_TYPE};

const COL_TYPE_SIZE: usize = 1;
const COL_NAME_LEN_SIZE: usize = 1;

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum ColType {
    Int(String),
    BigInt(String),
    Varchar(String, u16),
}

impl ColType {
    pub fn int(name: &str) -> Self {
        Self::Int(name.to_string())
    }

    pub fn bigint(name: &str) -> Self {
        Self::BigInt(name.to_string())
    }

    pub fn varchar(name: &str, size: u16) -> Self {
        Self::Varchar(name.to_string(), size)
    }

    pub fn col_type(&self) -> u8 {
        match self {
            Self::Int(_) => INT_TYPE,
            Self::BigInt(_) => BIG_INT_TYPE,
            Self::Varchar(_, _) => VARCHAR_TYPE,
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Self::Int(name) => name,
            Self::BigInt(name) => name,
            Self::Varchar(name, _) => name,
        }
    }
}

impl fmt::Display for ColType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(name) => write!(f, "{} INT", name),
            Self::BigInt(name) => write!(f, "{} BIGINT", name),
            Self::Varchar(name, size) => write!(f, "{} VARCHAR({})", name, size),
        }
    }
}

impl Pageable for ColType {
    fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        buffer[0] = self.col_type();
        let mut offset = 1;
        match self {
            Self::Int(name) => {
                let len = name.len();
                buffer[offset] = len as u8;
                offset += COL_NAME_LEN_SIZE;
                buffer[offset..offset + len].copy_from_slice(name.as_bytes());
                offset += len;
            }
            Self::BigInt(name) => {
                let len = name.len();
                buffer[offset] = len as u8;
                offset += COL_NAME_LEN_SIZE;
                buffer[offset..offset + len].copy_from_slice(name.as_bytes());
                offset += len;
            }
            Self::Varchar(name, size) => {
                let len = name.len();
                buffer[offset..offset + VARCHAR_LEN_SIZE].copy_from_slice(&size.to_be_bytes());
                offset += VARCHAR_LEN_SIZE;
                buffer[offset] = len as u8;
                offset += COL_NAME_LEN_SIZE;
                buffer[offset..offset + len].copy_from_slice(name.as_bytes());
                offset += len;
            }
        }
        Ok(offset)
    }

    fn read(buffer: &[u8]) -> Result<(Self, usize), DbError> {
        let mut offset = 0;
        let col_type = buffer[offset];
        offset += 1;
        match col_type {
            INT_TYPE => {
                let len = buffer[offset] as usize;
                offset += COL_NAME_LEN_SIZE;
                let mut name = vec![0u8; len];
                name.copy_from_slice(&buffer[offset..offset + len]);
                offset += len;
                let name = String::from_utf8_lossy(&name);
                Ok((Self::Int(name.to_string()), offset))
            }
            BIG_INT_TYPE => {
                let len = buffer[offset] as usize;
                offset += COL_NAME_LEN_SIZE;
                let mut name = vec![0u8; len];
                name.copy_from_slice(&buffer[offset..offset + len]);
                offset += len;
                let name = String::from_utf8_lossy(&name);
                Ok((Self::BigInt(name.to_string()), offset))
            }
            VARCHAR_TYPE => {
                let v_size = read_num!(buffer, u16, offset);
                offset += VARCHAR_LEN_SIZE;
                let len = buffer[offset] as usize;
                offset += COL_NAME_LEN_SIZE;
                let mut name = vec![0u8; len];
                name.copy_from_slice(&buffer[offset..offset + len]);
                offset += len;
                let name = String::from_utf8_lossy(&name);
                Ok((Self::Varchar(name.to_string(), v_size), offset))
            }
            _ => Err(DbError::Encoding),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Int(name) => COL_TYPE_SIZE + COL_NAME_LEN_SIZE + name.len(),
            Self::BigInt(name) => COL_TYPE_SIZE + COL_NAME_LEN_SIZE + name.len(),
            Self::Varchar(name, _) => {
                COL_TYPE_SIZE + VARCHAR_LEN_SIZE + COL_NAME_LEN_SIZE + name.len()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read() {
        let int = ColType::int("id");
        let mut buffer = vec![0u8; int.size()];
        int.write(&mut buffer).unwrap();
        let (restored, read) = ColType::read(&buffer).unwrap();
        assert_eq!(int.size(), read);
        assert_eq!(int, restored);

        let bigint = ColType::bigint("new_id");
        let mut buffer = vec![0u8; bigint.size()];
        bigint.write(&mut buffer).unwrap();
        let (restored, read) = ColType::read(&buffer).unwrap();
        assert_eq!(bigint.size(), read);
        assert_eq!(bigint, restored);

        let varchar = ColType::varchar("name", 10);
        let mut buffer = vec![0u8; varchar.size()];
        varchar.write(&mut buffer).unwrap();
        let (restored, read) = ColType::read(&buffer).unwrap();
        assert_eq!(varchar.size(), read);
        assert_eq!(varchar, restored);
    }

    #[test]
    fn display() {
        let int = ColType::int("id");
        assert_eq!(int.to_string(), "id INT");

        let bigint = ColType::bigint("timestamp");
        assert_eq!(bigint.to_string(), "timestamp BIGINT");

        let varchar = ColType::varchar("name", 16);
        assert_eq!(varchar.to_string(), "name VARCHAR(16)");
    }

    #[test]
    fn invalid_col_type() {
        let unknown = vec![255u8];
        let Err(DbError::Encoding) = ColType::read(&unknown) else {
            panic!("error not validated");
        };
    }

    #[test]
    fn get_name() {
        let col_type = ColType::int("id");
        assert_eq!("id", col_type.get_name());
    }
}
