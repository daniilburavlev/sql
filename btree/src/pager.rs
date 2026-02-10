use common::error::DbError;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::page::{Offset, PAGE_SIZE, PTR_SIZE, Page};

const HEADER_SIZE: usize = 128;

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
        Ok(Self {
            fd,
            cursor: HEADER_SIZE as u32,
        })
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
}
