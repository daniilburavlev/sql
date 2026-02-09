use common::{Pageable, error::DbError};

use crate::Col;

pub const ROW_COLS_SIZE: usize = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Row {
    pub columns: Vec<Col>,
}

#[macro_export]
macro_rules! row {
    [$cols:expr] => {
        Row { columns: vec![$cols] }
    };
}

impl Row {
    pub fn add_column(&mut self, column: Col) {
        self.columns.push(column);
    }
}

impl Pageable for Row {
    fn write(&self, buffer: &mut [u8]) -> Result<usize, DbError> {
        let mut offset = 0;
        buffer[offset] = self.columns.len() as u8;
        offset += ROW_COLS_SIZE;

        for column in self.columns.iter() {
            offset += column.write(&mut buffer[offset..])?;
        }
        Ok(offset)
    }

    fn read(buffer: &[u8]) -> Result<(Self, usize), DbError> {
        let mut offset = 0;
        let cols = buffer[offset];
        offset += ROW_COLS_SIZE;
        let mut columns = Vec::with_capacity(cols as usize);

        for _ in 0..cols {
            let (column, read) = Col::read(&buffer[offset..])?;
            offset += read;
            columns.push(column);
        }
        Ok((Row { columns }, offset))
    }

    fn size(&self) -> usize {
        let mut size = 0;
        size += ROW_COLS_SIZE;
        for col in self.columns.iter() {
            size += col.size();
        }
        size
    }
}

#[cfg(test)]
mod tests {
    use crate::col::{COL_TYPE_SIZE, INT_SIZE, VARCHAR_LEN_SIZE};

    use super::*;

    #[test]
    fn write_read() {
        let mut row = Row::default();
        row.add_column(Col::int(10));
        let varchar_len = 10;
        row.add_column(Col::varchar("Hello", varchar_len));
        let size = ROW_COLS_SIZE
            + COL_TYPE_SIZE
            + INT_SIZE
            + COL_TYPE_SIZE
            + 2 * VARCHAR_LEN_SIZE
            + varchar_len as usize;

        let mut buffer = [0u8; 21];
        row.write(&mut buffer).unwrap();

        let (row, read) = Row::read(&buffer).unwrap();
        assert_eq!(size, read);
        assert_eq!(
            row,
            Row {
                columns: vec![Col::Int(10), Col::varchar("Hello", 10)]
            }
        )
    }

    #[test]
    fn row_size() {
        let row = Row {
            columns: vec![Col::Int(10)],
        };
        let size = ROW_COLS_SIZE + COL_TYPE_SIZE + INT_SIZE;
        let r_size = row.size();
        assert_eq!(size, r_size);
    }
}
