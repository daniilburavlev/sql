use common::Pageable;

const ROW_TYPE_COLS_LEN_SIZE: usize = 1;

use crate::ColType;

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub struct RowType {
    pub columns: Vec<ColType>,
}

impl Pageable for RowType {
    fn write(&self, buffer: &mut [u8]) -> Result<usize, common::error::DbError> {
        let mut offset = 0;
        buffer[offset] = self.columns.len() as u8;
        offset += ROW_TYPE_COLS_LEN_SIZE;
        for col in self.columns.iter() {
            offset += col.write(&mut buffer[offset..])?;
        }
        Ok(offset)
    }

    fn read(buffer: &[u8]) -> Result<(Self, usize), common::error::DbError> {
        let mut offset = 0;
        let len = buffer[offset] as usize;
        offset += ROW_TYPE_COLS_LEN_SIZE;
        let mut columns = Vec::with_capacity(len);
        for _ in 0..len {
            let (col, read) = ColType::read(&buffer[offset..])?;
            offset += read;
            columns.push(col);
        }
        Ok((Self { columns }, offset))
    }

    fn size(&self) -> usize {
        let mut size = ROW_TYPE_COLS_LEN_SIZE;
        for col in self.columns.iter() {
            size += col.size();
        }
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_read() {
        let row = RowType {
            columns: vec![
                ColType::int("id"),
                ColType::bigint("timestamp"),
                ColType::varchar("name", 16),
            ],
        };
        let mut buffer = vec![0u8; row.size()];
        let write = row.write(&mut buffer).unwrap();
        assert_eq!(write, row.size());
        let (restored, read) = RowType::read(&buffer).unwrap();
        assert_eq!(read, row.size());
        assert_eq!(restored, row);
    }
}
