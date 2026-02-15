use common::{Pageable, error::DbError};
use row::RowType;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::page::{Offset, PAGE_SIZE, PTR_SIZE, Page};

pub const HEADER_SIZE: usize = 16 * 1024;

pub struct Pager {
    fd: File,
    cursor: Offset,
}

impl Pager {
    pub fn new(path: &Path) -> Result<Self, DbError> {
        let fd = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)?;
        let mut pager = Self {
            fd,
            cursor: HEADER_SIZE as u32,
        };
        pager.init()?;
        Ok(pager)
    }

    fn init(&mut self) -> Result<(), DbError> {
        let file_size = self.fd.seek(SeekFrom::End(0))?;
        self.cursor = file_size as u32;
        self.init_header(file_size)?;
        Ok(())
    }

    fn init_header(&mut self, file_size: u64) -> Result<(), DbError> {
        if file_size >= HEADER_SIZE as u64 {
            return Ok(());
        }
        let buffer = vec![0u8; HEADER_SIZE];
        self.fd.seek(SeekFrom::Start(0))?;
        self.fd.write_all(&buffer)?;
        self.cursor = HEADER_SIZE as u32;
        Ok(())
    }

    pub fn set_root(&mut self, offset: Offset) -> Result<(), DbError> {
        self.fd.seek(SeekFrom::Start(0))?;
        self.fd.write_all(&offset.to_be_bytes())?;
        self.fd.flush()?;
        Ok(())
    }

    pub fn get_root(&mut self) -> Result<Offset, DbError> {
        if self.fd.seek(SeekFrom::End(0))? == 0 {
            return Ok(0);
        }
        let mut buf = [0u8; PTR_SIZE];
        self.fd.seek(SeekFrom::Start(0))?;
        self.fd.read_exact(&mut buf)?;
        let offset = u32::from_be_bytes(buf);
        Ok(offset)
    }

    pub fn get_page(&mut self, offset: Offset) -> Result<Page, DbError> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        self.fd.seek(SeekFrom::Start(offset as u64))?;
        self.fd.read_exact(&mut buffer)?;
        buffer.try_into()
    }

    pub fn write_page(&mut self, page: Page) -> Result<Offset, DbError> {
        let offset = self.cursor;
        self.fd.seek(SeekFrom::Start(self.cursor as u64))?;
        let buffer: Vec<u8> = page.try_into()?;
        self.fd.write_all(&buffer)?;
        self.fd.flush()?;
        self.cursor += PAGE_SIZE as u32;
        Ok(offset)
    }

    pub fn write_page_at_offset(&mut self, page: Page, offset: Offset) -> Result<(), DbError> {
        self.fd.seek(SeekFrom::Start(offset as u64))?;
        let buffer: Vec<u8> = page.try_into()?;
        self.fd.write_all(&buffer)?;
        self.fd.flush()?;
        Ok(())
    }

    pub fn get_offset(&self) -> Offset {
        self.cursor
    }

    pub fn get_next_offset(&self) -> Offset {
        self.cursor + (PAGE_SIZE as u32)
    }

    pub fn set_structure(&mut self, row_type: RowType) -> Result<(), DbError> {
        let len = row_type.size();
        self.fd.seek(SeekFrom::Start(PTR_SIZE as u64))?;
        let mut buffer = vec![0u8; len];
        row_type.write(&mut buffer)?;
        self.fd.write_all(&buffer)?;
        Ok(())
    }

    pub fn get_structure(&mut self) -> Result<RowType, DbError> {
        self.fd.seek(SeekFrom::Start(PTR_SIZE as u64))?;
        let mut buffer = vec![0u8; HEADER_SIZE - PTR_SIZE];
        self.fd.read_exact(&mut buffer)?;
        let (row_type, _) = RowType::read(&buffer)?;
        Ok(row_type)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn cursor() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut pager = Pager::new(tmpfile.path()).unwrap();
        pager
            .write_page(Page::Leaf {
                parent: 0,
                values: vec![],
            })
            .unwrap();
        let cursor1 = pager.cursor;
        let pager = Pager::new(tmpfile.path()).unwrap();
        let cursor2 = pager.cursor;
        assert_eq!(cursor1, cursor2);
    }
}
