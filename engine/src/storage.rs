use std::path::PathBuf;

use btree::BTree;
use common::error::DbError;
use row::{Col, Row};

use crate::result::ExecResult;

pub(crate) struct Storage {
    path: PathBuf,
}

impl Storage {
    pub(crate) fn create(&self, name: &str) -> Result<ExecResult, DbError> {
        let path = self.table_path(name);
        BTree::new(&path)?;
        Ok(ExecResult::ok("create", 1))
    }

    pub(crate) fn insert(
        &self,
        name: &str,
        values: Vec<(Col, Row)>,
    ) -> Result<ExecResult, DbError> {
        let path = self.table_path(name);
        let mut btree = BTree::new(&path)?;
        let len = values.len();
        for (key, value) in values {
            btree.insert(key, value)?;
        }
        Ok(ExecResult::ok("inserted", len as i32))
    }

    pub(crate) fn select(&self, name: &str) -> Result<ExecResult, DbError> {
        let path = self.table_path(name);
        let btree = BTree::new(&path)?;
        Ok(ExecResult::ok("inserted", 0))
    }

    fn table_path(&self, table_name: &str) -> PathBuf {
        let mut path = self.path.clone();
        path.push(table_name);
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {}
}

