use common::error::DbError;

use crate::col::{COL_TYPE_SIZE, Col, VARCHAR_LEN_SIZE};
use std::collections::HashMap;

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

    pub fn write_header(&mut self, buffer: &mut [u8]) -> Result<(), DbError> {
        let mut offset = 0;
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
        Ok(())
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
        let mut buffer = [0u8; 4];
        row.write_header(&mut buffer[0..4]).unwrap();
    }
}
