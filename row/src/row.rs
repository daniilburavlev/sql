use common::error::DbError;

use crate::col::{BIGINT_SIZE, COL_TYPE_SIZE, Col, ColType, INT_SIZE, VARCHAR_LEN_SIZE};

pub const ROW_COLS_SIZE: usize = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Row {
    pub columns: Vec<Col>,
}

impl Row {
    pub fn add_column(&mut self, column: Col) {
        self.columns.push(column);
    }

    pub fn write_header(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        let mut offset = 0;
        buffer[offset..ROW_COLS_SIZE].copy_from_slice(&(self.columns.len() as u8).to_be_bytes());
        offset += ROW_COLS_SIZE;
        for col in self.columns.iter() {
            let col_type = col.get_type();
            buffer[offset..offset + COL_TYPE_SIZE].copy_from_slice(&col_type.to_be_bytes());
            offset += COL_TYPE_SIZE;
            if let Col::Varchar(_, size) = col {
                buffer[offset..offset + VARCHAR_LEN_SIZE]
                    .copy_from_slice(&(*size as u16).to_be_bytes());
                offset += VARCHAR_LEN_SIZE;
            }
        }
        Ok(offset)
    }

    pub fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        let mut offset = 0;
        for col in self.columns.iter() {
            offset += col.write(&mut buffer[offset..])?;
        }
        Ok(offset)
    }

    pub fn read_header(buffer: &[u8]) -> Result<(Vec<ColType>, usize), DbError> {
        let mut offset = 0;
        let cols = buffer[offset];
        offset += ROW_COLS_SIZE;

        let mut col_types = Vec::with_capacity(cols as usize);
        for _ in 0..cols {
            let mut col_type: ColType = buffer[offset].try_into()?;
            offset += COL_TYPE_SIZE;
            if let ColType::Varchar(_) = col_type {
                let mut len = [0u8; VARCHAR_LEN_SIZE];
                len.copy_from_slice(&buffer[offset..offset + VARCHAR_LEN_SIZE]);
                let len = u16::from_be_bytes(len);
                col_type = ColType::Varchar(len as usize);
                offset += VARCHAR_LEN_SIZE;
            }
            col_types.push(col_type);
        }
        Ok((col_types, offset))
    }

    pub fn read(header: &[ColType], buffer: &mut [u8]) -> Result<(Self, usize), DbError> {
        let mut columns = Vec::with_capacity(header.len());
        let mut offset = 0;
        for col_type in header.iter() {
            match col_type {
                ColType::Int => {
                    columns.push(Col::parse_int(&buffer[offset..offset + INT_SIZE])?);
                    offset += INT_SIZE;
                }
                ColType::BigInt => {
                    columns.push(Col::parse_bigint(&buffer[offset..offset + BIGINT_SIZE])?);
                    offset += BIGINT_SIZE;
                }
                ColType::Varchar(size) => {
                    columns.push(Col::parse_varchar(
                        &buffer[offset..offset + VARCHAR_LEN_SIZE + *size],
                        *size,
                    )?);
                    offset += VARCHAR_LEN_SIZE + *size;
                }
                ColType::Link => {}
            }
        }
        Ok((Row { columns }, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read_header() {
        let mut row = Row::default();
        row.add_column(Col::int(10));
        row.add_column(Col::varchar("Hello", 10));
        let mut buffer = [0u8; 5];
        row.write_header(&mut buffer[0..5]).unwrap();

        let (header, size) = Row::read_header(&buffer).unwrap();
        assert_eq!(size, 5);
        assert_eq!(vec![ColType::Int, ColType::Varchar(10)], header);
    }

    #[test]
    fn write_read() {
        let mut row = Row::default();
        row.add_column(Col::int(10));
        row.add_column(Col::varchar("Hello", 10));
        let mut buffer = [0u8; 16];
        row.write(&mut buffer[0..16]).unwrap();
        let header = vec![ColType::Int, ColType::Varchar(10)];

        let (row, size) = Row::read(&header, &mut buffer).unwrap();
        assert_eq!(size, 16);
        assert_eq!(
            row,
            Row {
                columns: vec![Col::Int(10), Col::varchar("Hello", 10)]
            }
        )
    }
}
