use std::path::Path;

use crate::page::PAGE_SIZE;
use crate::{
    error::DbError,
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

    pub fn insert(&mut self, key: String, value: String) -> Result<(), DbError> {
        let mut offset = self.pager.get_root()?;
        let mut page = self.pager.get_page(offset)?;
        let mut new_key_offset = None::<(String, Offset)>;

        loop {
            match page {
                Page::Node {
                    parent,
                    mut children,
                } => {
                    if let Some((key, child_offset)) = new_key_offset.take() {
                        insert_key_value(&mut children, (key.clone(), child_offset));
                        if Page::node_size(&children) <= PAGE_SIZE {
                            let page = Page::Node { parent, children };
                            self.pager.write_page_at_offset(page, offset)?;
                            break;
                        }
                        let mid = children.len() / 2;
                        let right_children = children.split_off(mid);
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
                    let key_value = (key.clone(), value.clone());
                    insert_key_value(&mut values, key_value);
                    if Page::leaf_size(&values) <= PAGE_SIZE {
                        let page = Page::Leaf { parent, values };
                        self.pager.write_page_at_offset(page, offset)?;
                        break;
                    }
                    let mid = values.len() / 2;
                    let right_values = values.split_off(mid);
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

    pub fn search(&mut self, key: String) -> Result<Option<String>, DbError> {
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

    pub fn delete(&mut self, key: String) -> Result<Option<String>, DbError> {
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
        right_children: &[(String, u32)],
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

fn insert_key_value<T>(values: &mut Vec<(String, T)>, value: (String, T)) {
    let idx = values
        .binary_search_by(|kv| kv.0.cmp(&value.0))
        .unwrap_or_else(|x| x);
    if idx < values.len() && values[idx].0 == value.0 {
        values[idx] = value;
    } else if idx >= values.len() {
        values.push(value);
    } else {
        values.insert(idx, value);
    }
}

fn get_index<T>(values: &[(String, T)], value: &String) -> usize {
    values
        .binary_search_by(|kv| kv.0.cmp(value))
        .unwrap_or_else(|x| if x == 0 { 0 } else { x - 1 })
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn two_leaf_one_node() {
        let tempfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tempfile.path()).unwrap();
        for i in 0..2 {
            let key = i.to_string().repeat(1024);
            let value = i.to_string().repeat(2048);
            btree.insert(key, value).unwrap();
        }
        let mut pager = Pager::new(tempfile.path()).unwrap();
        let left_leaf = pager.get_page(128).unwrap();
        let root_node = pager.get_page((128 + PAGE_SIZE) as u32).unwrap();
        let right_leaf = pager
            .get_page((128 + PAGE_SIZE + PAGE_SIZE) as u32)
            .unwrap();
        match left_leaf {
            Page::Leaf { parent, values } => {
                assert_eq!(parent, (128 + PAGE_SIZE) as u32);
                assert_eq!(1, values.len());
            }
            _ => panic!("Unexpected node page"),
        }
        match root_node {
            Page::Node { parent, children } => {
                assert_eq!(parent, 0);
                assert_eq!(2, children.len());
                assert_eq!(128, children[0].1);
                assert_eq!((128 + PAGE_SIZE + PAGE_SIZE) as u32, children[1].1);
            }
            _ => panic!("Unexpected leaf page"),
        }
        match right_leaf {
            Page::Leaf { parent, values } => {
                assert_eq!(parent, (128 + PAGE_SIZE) as u32);
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
            let key = i.to_string().repeat(2000);
            let value = i.to_string().repeat(2000);
            btree.insert(key, value).unwrap();
        }
        let mut pager = Pager::new(tempfile.path()).unwrap();
        let mut offset = 128;
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
            btree.insert(i.to_string(), i.to_string()).unwrap();
        }
        for i in 0..1000 {
            let value = btree.search(i.to_string()).unwrap();
            assert_eq!(value.unwrap(), i.to_string());
        }
        for i in 0..1000 {
            btree.insert(i.to_string(), 0.to_string()).unwrap();
        }
        for i in 0..1000 {
            let value = btree.search(i.to_string()).unwrap();
            assert_eq!(value.unwrap(), 0.to_string());
        }
    }

    #[test]
    fn insert_key_value_test() {
        let mut offsets = vec![("0".to_string(), 1), ("237".to_string(), 2)];
        insert_key_value(&mut offsets, ("325".to_string(), 3));
        assert_eq!(
            vec![
                ("0".to_string(), 1),
                ("237".to_string(), 2),
                ("325".to_string(), 3),
            ],
            offsets,
        );
    }

    #[test]
    fn insert_key_value_in_order() {
        let mut values = vec![];
        let mut key_value = vec![];
        for i in 0..1000 {
            values.push((i.to_string(), i.to_string()));
            insert_key_value(&mut key_value, (i.to_string(), i.to_string()));
        }
        values.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(values, key_value);
    }

    #[test]
    fn insert_delete_key() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        for i in 0..1000 {
            btree.insert(i.to_string(), i.to_string()).unwrap();
            if i % 2 == 0 {
                let result = btree.delete(i.to_string()).unwrap();
                assert_eq!(result.unwrap(), i.to_string());
            }
        }
        for i in 0..1000 {
            let result = btree.search(i.to_string()).unwrap();
            if i % 2 == 0 {
                assert_eq!(result, None);
            } else {
                assert_eq!(result, Some(i.to_string()));
            }
        }
    }

    #[test]
    fn delete_not_existed() {
        let tmpfile = NamedTempFile::new().unwrap();
        let mut btree = BTree::new(tmpfile.path()).unwrap();
        let response = btree.delete(0.to_string()).unwrap();
        assert_eq!(response, None);
    }
}
