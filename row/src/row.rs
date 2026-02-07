use common::error::DbError;

use crate::col::{COL_TYPE_SIZE, Col, VARCHAR_LEN_SIZE};
use std::collections::HashMap;

pub const ROW_COLS_SIZE: usize = 1;

#[derive(Clone, Default)]
pub struct Row {
    pub col_names: Vec<String>,
    pub col: HashMap<String, Col>,
}

impl Row {
    pub fn add_column(&mut self, name: String, column: Col) {
        self.col.insert(name.clone(), column);
        self.col_names.push(name);
    }

    pub fn write_header(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        let mut offset = 0;
        buffer[offset..ROW_COLS_SIZE].copy_from_slice(&(self.col_names.len() as u8).to_be_bytes());
        offset += ROW_COLS_SIZE;

        for col_name in self.col_names.iter() {
            if let Some(col) = self.col.get(col_name) {
                let col_type = col.get_type();
                buffer[offset..offset + COL_TYPE_SIZE].copy_from_slice(&col_type.to_be_bytes());
                offset += COL_TYPE_SIZE;
                if let Col::Varchar(_, size) = col {
                    buffer[offset..offset + VARCHAR_LEN_SIZE]
                        .copy_from_slice(&(*size as u16).to_be_bytes());
                    offset += VARCHAR_LEN_SIZE;
                }
            }
        }
        Ok(offset)
    }

    pub fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        let mut offset = 0;
        for col_name in self.col_names.iter() {
            if let Some(col) = self.col.get(col_name) {
                offset += col.write(&mut buffer[offset..])?;
            }
        }
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_row_header() {
        let mut row = Row::default();
        row.add_column("id".to_string(), Col::int(10));
        row.add_column("name".to_string(), Col::varchar("Hello", 10));
        let mut buffer = [0u8; 5];
        row.write_header(&mut buffer[0..5]).unwrap();
    }

    #[test]
    fn write_row() {
        let mut row = Row::default();
        row.add_column("id".to_string(), Col::int(10));
        row.add_column("name".to_string(), Col::varchar("Hello", 10));
        let mut buffer = [0u8; 11];
        row.write(&mut buffer[0..11]).unwrap();
    }
}
