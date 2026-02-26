use std::path::{Path, PathBuf};

use btree::BTree;
use common::error::DbError;
use row::{Col, Row, RowType};

pub(crate) struct Storage {
    path: PathBuf,
}

impl Storage {
    pub(crate) fn new(path: &Path) -> Result<Self, DbError> {
        Ok(Self {
            path: PathBuf::from(path),
        })
    }

    pub(crate) fn get_row_type(&self, name: &str) -> Result<RowType, DbError> {
        let path = self.table_path(name);
        let mut btree = BTree::new(&path)?;
        btree.get_structure()
    }

    pub(crate) fn create(&self, name: &str, row_type: RowType) -> Result<usize, DbError> {
        let path = self.table_path(name);
        let mut btree = BTree::new(&path)?;
        btree.set_structure(row_type)?;
        Ok(1)
    }

    pub(crate) fn insert(&self, name: &str, values: Vec<(Col, Row)>) -> Result<usize, DbError> {
        let path = self.table_path(name);
        let mut btree = BTree::new(&path)?;
        let len = values.len();
        for (key, value) in values {
            btree.insert(key, value)?;
        }
        Ok(len)
    }

    pub(crate) fn select_all(&self, name: &str) -> Result<Vec<Row>, DbError> {
        let path = self.table_path(name);
        let mut btree = BTree::new(&path)?;
        btree.select_all()
    }

    pub(crate) fn delete_all(&self, name: &str) -> Result<i32, DbError> {
        let path = self.table_path(name);
        let mut btree = BTree::new(&path)?;
        btree.delete_all()
    }

    fn table_path(&self, table_name: &str) -> PathBuf {
        let mut path = self.path.clone();
        path.push(table_name);
        path
    }
}

#[cfg(test)]
mod tests {

    use row::ColType;

    use super::*;

    #[test]
    fn create() {
        let temp_dir = tempfile::tempdir().unwrap();
        let name = "test";
        let storage = Storage::new(temp_dir.path()).unwrap();
        let row_type = row::row_type![ColType::int("id")];
        storage.create(name, row_type.clone()).unwrap();
        let header = storage.get_row_type(name).unwrap();
        assert_eq!(header, row_type);
    }

    #[test]
    fn insert() {
        let temp_dir = tempfile::tempdir().unwrap();
        let name = "test";
        let storage = Storage::new(temp_dir.path()).unwrap();
        let row_type = row::row_type![ColType::int("id")];
        storage.create(name, row_type.clone()).unwrap();
        storage
            .insert(name, vec![(Col::int(10), row::row![Col::int(10)])])
            .unwrap();
    }

    #[test]
    fn select_all() {
        let temp_dir = tempfile::tempdir().unwrap();
        let name = "test";
        let storage = Storage::new(temp_dir.path()).unwrap();
        let row_type = row::row_type![ColType::int("id")];
        storage.create(name, row_type.clone()).unwrap();
        storage
            .insert(name, vec![(Col::int(10), row::row![Col::int(10)])])
            .unwrap();
        let rows = storage.select_all(name).unwrap();
        assert_eq!(1, rows.len());
    }
}
