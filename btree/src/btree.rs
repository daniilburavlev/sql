use std::path::Path;

use common::Pageable;
use common::error::DbError;
use row::{Col, Row, RowType};

use crate::page::{
    MAX_KEY_VALUE_SIZE, PAGE_SIZE, get_index, insert_key_value, split_leaf, split_node,
};

use crate::pager::HEADER_SIZE;
use crate::{
    page::{Offset, Page},
    pager::Pager,
};

pub struct BTree {
    pager: Pager,
}

impl BTree {
    pub fn new(path: &Path) -> Result<Self, DbError> {
        let mut pager = Pager::new(path)?;
        let mut root_offset = pager.get_root()?;
        if root_offset == 0 {
            let page = Page::Leaf {
                parent: 0,
                values: vec![],
            };
            root_offset = pager.write_page(page)?;
            pager.set_root(root_offset)?;
        }
        Ok(Self { pager })
    }

    pub fn set_structure(&mut self, row_type: RowType) -> Result<(), DbError> {
        self.pager.set_structure(row_type)?;
        Ok(())
    }

    pub fn get_structure(&mut self) -> Result<RowType, DbError> {
        self.pager.get_structure()
    }

    pub fn insert(&mut self, key: Col, value: Row) -> Result<(), DbError> {
        let mut offset = self.pager.get_root()?;
        let mut page = self.pager.get_page(offset)?;
        let mut new_key_offset = None::<(Col, Offset)>;

        loop {
            match page {
                Page::Node {
                    parent,
                    mut children,
                } => {
                    if let Some((key, child_offset)) = new_key_offset.take() {
                        insert_key_value(&mut children, (key, child_offset));
                        if Page::node_size(&children) <= PAGE_SIZE {
                            let page = Page::Node { parent, children };
                            self.pager.write_page_at_offset(page, offset)?;
                            break;
                        }
                        let (children, right_children) = split_node(children);
                        let left_key = children[0].0.clone();
                        let right_key = right_children[0].0.clone();
                        if parent == 0 {
                            let parent = self.pager.get_offset();
                            let right_offset = self.pager.get_next_offset();
                            let left = Page::Node { parent, children };
                            self.rewrite_parent(right_offset, &right_children)?;
                            let right = Page::Node {
                                parent,
                                children: right_children,
                            };
                            let page = Page::Node {
                                parent: 0,
                                children: vec![(left_key, child_offset), (right_key, right_offset)],
                            };
                            self.pager.set_root(parent)?;
                            self.pager.write_page_at_offset(left, offset)?;
                            self.pager.write_page(page)?;
                            self.pager.write_page(right)?;
                            break;
                        }
                        offset = parent;
                        page = self.pager.get_page(parent)?;
                        let right = Page::Node {
                            parent,
                            children: right_children.clone(),
                        };
                        let right_offset = self.pager.write_page(right)?;
                        self.rewrite_parent(right_offset, &right_children)?;
                        new_key_offset = Some((right_key, right_offset));
                    } else {
                        let idx = get_index(&children, &key);
                        let (_, child_offset) = children[idx];
                        page = self.pager.get_page(child_offset)?;
                        offset = child_offset;
                    }
                }
                Page::Leaf { parent, mut values } => {
                    let kv_size = key.size() + value.size();
                    if kv_size > MAX_KEY_VALUE_SIZE {
                        return Err(DbError::MaxSize(kv_size, MAX_KEY_VALUE_SIZE));
                    }
                    let key_value = (key.clone(), value.clone());
                    insert_key_value(&mut values, key_value);
                    if Page::leaf_size(&values) <= PAGE_SIZE {
                        let page = Page::Leaf { parent, values };
                        self.pager.write_page_at_offset(page, offset)?;
                        break;
                    }
                    let (values, right_values) = split_leaf(values);
                    let left_key = values[0].0.clone();
                    let right_key = right_values[0].0.clone();
                    if parent == 0 {
                        let parent = self.pager.get_offset();
                        let right_offset = self.pager.get_next_offset();
                        let left = Page::Leaf { parent, values };
                        let right = Page::Leaf {
                            parent,
                            values: right_values,
                        };
                        let page = Page::Node {
                            parent: 0,
                            children: vec![(left_key, offset), (right_key, right_offset)],
                        };
                        self.pager.set_root(parent)?;
                        self.pager.write_page_at_offset(left, offset)?;
                        self.pager.write_page(page)?;
                        self.pager.write_page(right)?;
                        break;
                    } else {
                        let left = Page::Leaf { parent, values };
                        self.pager.write_page_at_offset(left, offset)?;
                        offset = parent;
                        page = self.pager.get_page(parent)?;
                        let right = Page::Leaf {
                            parent,
                            values: right_values,
                        };
                        let right_offset = self.pager.write_page(right)?;
                        new_key_offset = Some((right_key, right_offset));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn search(&mut self, key: Col) -> Result<Option<Row>, DbError> {
        let offset: Offset = self.pager.get_root()?;
        let mut page = self.pager.get_page(offset)?;
        loop {
            match page {
                Page::Node { children, .. } => {
                    let idx = get_index(&children, &key);
                    let (_, offset) = children[idx];
                    page = self.pager.get_page(offset)?;
                }
                Page::Leaf { values, .. } => {
                    return match values.binary_search_by(|kv| kv.0.cmp(&key)) {
                        Ok(idx) => Ok(Some(values[idx].1.clone())),
                        Err(_) => Ok(None),
                    };
                }
            }
        }
    }

    pub fn select_all(&mut self) -> Result<Vec<Row>, DbError> {
        let mut offset = HEADER_SIZE as u32;
        let latest_offset = self.pager.get_offset();

        let mut rows = Vec::new();
        while offset < latest_offset {
            match self.pager.get_page(offset)? {
                Page::Node { .. } => {}
                Page::Leaf { values, .. } => {
                    for (_, row) in values {
                        rows.push(row);
                    }
                }
            }
            offset += PAGE_SIZE as u32;
        }
        Ok(rows)
    }

    pub fn delete(&mut self, key: Col) -> Result<Option<Row>, DbError> {
        let mut offset = self.pager.get_root()?;
        let mut page = self.pager.get_page(offset)?;
        loop {
            match page {
                Page::Leaf { parent, mut values } => {
                    return match values.binary_search_by(|kv| kv.0.cmp(&key)) {
                        Ok(idx) => {
                            let value = values.remove(idx);
                            let page = Page::Leaf { parent, values };
                            self.pager.write_page_at_offset(page, offset)?;
                            Ok(Some(value.1))
                        }
                        Err(_) => Ok(None),
                    };
                }
                Page::Node { children, .. } => {
                    let idx = get_index(&children, &key);
                    offset = children[idx].1;
                    page = self.pager.get_page(offset)?;
                }
            }
        }
    }

    fn rewrite_parent(
        &mut self,
        right_offset: u32,
        right_children: &[(Col, Offset)],
    ) -> Result<(), DbError> {
        for (_, child_offset) in right_children.iter() {
            let updated_page = match self.pager.get_page(*child_offset)? {
                Page::Node { children, .. } => Page::Node {
                    parent: right_offset,
                    children,
                },
                Page::Leaf { values, .. } => Page::Leaf {
                    parent: right_offset,
                    values,
                },
            };
            self.pager
                .write_page_at_offset(updated_page, *child_offset)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use row::{ColType, row};
    use tempfile::NamedTempFile;

    use crate::pager::HEADER_SIZE;

    use super::*;

    #[test]
    fn two_leaf_one_node() {
        let tempfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tempfile.path()).unwrap();
        for i in 0..2 {
            let key = Col::varchar(&i.to_string(), 1024);
            let value = row![Col::varchar(&i.to_string(), 2048)];
            btree.insert(key, value).unwrap();
        }
        let mut pager = Pager::new(tempfile.path()).unwrap();
        let left_leaf = pager.get_page(HEADER_SIZE as u32).unwrap();
        let root_node = pager.get_page((HEADER_SIZE + PAGE_SIZE) as u32).unwrap();
        let right_leaf = pager
            .get_page((HEADER_SIZE + PAGE_SIZE + PAGE_SIZE) as u32)
            .unwrap();
        match left_leaf {
            Page::Leaf { parent, values } => {
                assert_eq!(parent, (HEADER_SIZE + PAGE_SIZE) as u32);
                assert_eq!(1, values.len());
            }
            _ => panic!("Unexpected node page"),
        }
        match root_node {
            Page::Node { parent, children } => {
                assert_eq!(parent, 0);
                assert_eq!(2, children.len());
                assert_eq!(HEADER_SIZE as u32, children[0].1);
                assert_eq!((HEADER_SIZE + PAGE_SIZE + PAGE_SIZE) as u32, children[1].1);
            }
            _ => panic!("Unexpected leaf page"),
        }
        match right_leaf {
            Page::Leaf { parent, values } => {
                assert_eq!(parent, (HEADER_SIZE + PAGE_SIZE) as u32);
                assert_eq!(1, values.len());
            }
            _ => panic!("Unexpected node page"),
        }
    }

    #[test]
    fn node_split() {
        let tempfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tempfile.path()).unwrap();
        for i in 0..4 {
            let key = Col::varchar(&i.to_string(), 2000);
            let value = row![Col::varchar(&i.to_string(), 2000)];
            btree.insert(key, value).unwrap();
        }
        let mut pager = Pager::new(tempfile.path()).unwrap();
        let mut offset = HEADER_SIZE as u32;
        for _ in 0..10 {
            pager.get_page(offset).unwrap();
            offset += PAGE_SIZE as u32;
        }
    }

    #[test]
    fn insert_1000() {
        let tempfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tempfile.path()).unwrap();
        for i in 0..1000 {
            btree
                .insert(
                    Col::varchar(&i.to_string(), 4),
                    row![Col::varchar(&i.to_string(), 4)],
                )
                .unwrap();
        }
        for i in 0..1000 {
            let value = btree.search(Col::varchar(&i.to_string(), 4)).unwrap();
            assert_eq!(value.unwrap(), row![Col::varchar(&i.to_string(), 4)]);
        }
        for i in 0..1000 {
            btree
                .insert(
                    Col::varchar(&i.to_string(), 4),
                    row![Col::varchar(&0.to_string(), 4)],
                )
                .unwrap();
        }
        for i in 0..1000 {
            let value = btree.search(Col::varchar(&i.to_string(), 4)).unwrap();
            assert_eq!(value.unwrap(), row![Col::varchar(&0.to_string(), 4)]);
        }
    }

    #[test]
    fn insert_delete_key() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        for i in 0..1000 {
            btree
                .insert(
                    Col::varchar(&i.to_string(), 4),
                    row![Col::varchar(&i.to_string(), 4)],
                )
                .unwrap();
            if i % 2 == 0 {
                let result = btree.delete(Col::varchar(&i.to_string(), 4)).unwrap();
                assert_eq!(result.unwrap(), row![Col::varchar(&i.to_string(), 4)]);
            }
        }
        for i in 0..1000 {
            let result = btree.search(Col::varchar(&i.to_string(), 4)).unwrap();
            if i % 2 == 0 {
                assert_eq!(result, None);
            } else {
                assert_eq!(result, Some(row![Col::varchar(&i.to_string(), 4)]));
            }
        }
    }

    #[test]
    fn delete_not_existed() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        let response = btree.delete(Col::varchar(&0.to_string(), 4)).unwrap();
        assert_eq!(response, None);
    }

    #[test]
    fn insert_huge_key() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        let key = Col::varchar(&0.to_string(), PAGE_SIZE as u16);
        let Err(DbError::MaxSize(received, limit)) =
            btree.insert(key, row![Col::varchar(&0.to_string(), 4)])
        else {
            panic!("size hasn't been validated")
        };
        assert_eq!(received, 4111);
        assert_eq!(limit, MAX_KEY_VALUE_SIZE);
    }

    #[test]
    fn set_get_structure() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        let row_type = RowType {
            columns: vec![ColType::int("id"), ColType::varchar("name", 16)],
        };
        btree.set_structure(row_type.clone()).unwrap();
        let saved = btree.get_structure().unwrap();
        assert_eq!(row_type, saved);
    }

    #[test]
    fn select_all() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        for i in 0..100 {
            btree.insert(Col::int(i), row![Col::int(20)]).unwrap();
        }
        let rows = btree.select_all().unwrap();
        assert_eq!(100, rows.len());
        for row in rows {
            assert_eq!(row![Col::int(20)], row);
        }
    }
}
