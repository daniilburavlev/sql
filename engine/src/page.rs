use common::{Pageable, error::DbError, read_num};
use row::{Col, Row};

pub(crate) const PAGE_SIZE: usize = 4 * 1024;
pub(crate) const LEN_SIZE: usize = 2;
pub(crate) const PTR_SIZE: usize = 4;
pub(crate) const MAX_KEY_VALUE_SIZE: usize = PAGE_SIZE - TYPE_SIZE - PTR_SIZE - LEN_SIZE;

const TYPE_SIZE: usize = 1;

pub type Offset = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Page {
    Node {
        parent: u32,
        children: Vec<(Col, Offset)>,
    },
    Leaf {
        parent: u32,
        values: Vec<(Col, Row)>,
    },
}

impl Page {
    pub fn page_type(&self) -> u8 {
        match self {
            Self::Node { .. } => 1,
            Self::Leaf { .. } => 2,
        }
    }

    pub fn leaf_size(values: &Vec<(Col, Row)>) -> usize {
        let mut size = TYPE_SIZE + PTR_SIZE + LEN_SIZE;
        for (k, v) in values {
            size += k.size();
            size += v.size();
        }
        size
    }

    pub fn node_size(values: &Vec<(Col, Offset)>) -> usize {
        let mut size = TYPE_SIZE + PTR_SIZE + LEN_SIZE;
        for (key, _) in values {
            size += key.size();
            size += PTR_SIZE;
        }
        size
    }
}

impl TryFrom<Vec<u8>> for Page {
    type Error = DbError;

    fn try_from(buffer: Vec<u8>) -> Result<Self, Self::Error> {
        let mut offset = 0;
        let page_type = buffer[offset];
        offset += TYPE_SIZE;

        let parent = read_num!(buffer, u32, offset);
        offset += PTR_SIZE;

        let elements = read_num!(buffer, u16, offset);
        offset += LEN_SIZE;

        match page_type {
            1 => {
                let mut children = Vec::new();
                for _ in 0..elements {
                    let (key, read) = Col::read(&buffer[offset..])?;
                    offset += read;

                    let pointer = read_num!(buffer, u32, offset);
                    offset += PTR_SIZE;

                    children.push((key, pointer));
                }
                Ok(Self::Node { parent, children })
            }
            2 => {
                let mut values = Vec::new();
                for _ in 0..elements {
                    let (key, read) = Col::read(&buffer[offset..])?;
                    offset += read;

                    let (value, read) = Row::read(&buffer[offset..])?;
                    offset += read;

                    values.push((key, value));
                }
                Ok(Self::Leaf { parent, values })
            }
            _ => Err(DbError::Encoding),
        }
    }
}

impl TryInto<Vec<u8>> for Page {
    type Error = DbError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let mut buffer = vec![0u8; PAGE_SIZE];
        let mut offset = 0;

        let page_type = self.page_type();
        buffer[offset] = page_type;
        offset += TYPE_SIZE;

        match self {
            Self::Node { parent, children } => {
                if Self::node_size(&children) > PAGE_SIZE {
                    return Err(DbError::Encoding);
                }
                buffer[offset..offset + PTR_SIZE].copy_from_slice(&parent.to_be_bytes());
                offset += PTR_SIZE;

                buffer[offset..offset + LEN_SIZE]
                    .copy_from_slice(&(children.len() as u16).to_be_bytes());
                offset += LEN_SIZE;

                for (key, pointer) in children {
                    offset += key.write(&mut buffer[offset..])?;

                    buffer[offset..offset + PTR_SIZE].copy_from_slice(&pointer.to_be_bytes());
                    offset += PTR_SIZE;
                }
            }
            Self::Leaf { parent, values } => {
                if Self::leaf_size(&values) > PAGE_SIZE {
                    return Err(DbError::Encoding);
                }
                buffer[offset..offset + PTR_SIZE].copy_from_slice(&parent.to_be_bytes());
                offset += PTR_SIZE;

                buffer[offset..offset + LEN_SIZE]
                    .copy_from_slice(&(values.len() as u16).to_be_bytes());
                offset += LEN_SIZE;

                for (key, value) in values {
                    offset += key.write(&mut buffer[offset..])?;

                    offset += value.write(&mut buffer[offset..])?;
                }
            }
        }
        Ok(buffer)
    }
}

pub fn insert_key_value<T>(values: &mut Vec<(Col, T)>, value: (Col, T)) {
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

pub fn get_index<T>(values: &[(Col, T)], value: &Col) -> usize {
    values
        .binary_search_by(|kv| kv.0.cmp(value))
        .unwrap_or_else(|x| if x == 0 { 0 } else { x - 1 })
}

pub type Splitted<T> = (Vec<(Col, T)>, Vec<(Col, T)>);

pub fn split_leaf(mut values: Vec<(Col, Row)>) -> Splitted<Row> {
    let mid = values.len() / 2;
    let mut right = values.split_off(mid);
    let mut size = Page::leaf_size(&right);
    while size > MAX_KEY_VALUE_SIZE {
        let value = right.remove(0);
        size -= value.0.size() + value.1.size();
        values.push(value);
    }
    (values, right)
}

pub fn split_node(mut values: Vec<(Col, Offset)>) -> Splitted<Offset> {
    let mid = values.len() / 2;
    let mut right = values.split_off(mid);
    let mut size = Page::node_size(&right);
    while size > MAX_KEY_VALUE_SIZE {
        let value = right.remove(0);
        size -= value.0.size() + PTR_SIZE;
        values.push(value);
    }
    (values, right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use row::row;

    #[test]
    fn write_read() {
        let node = Page::Node {
            parent: 1337,
            children: vec![
                (Col::int(1), 10),
                (Col::int(2), 11),
                (Col::int(3), 3),
                (Col::int(4), 4),
            ],
        };
        let buffer: Vec<u8> = node.clone().try_into().unwrap();
        let restored: Page = buffer.try_into().unwrap();
        assert_eq!(restored, node);
    }

    #[test]
    fn leaf_node_convert() {
        let leaf = Page::Leaf {
            parent: 1338,
            values: vec![
                (Col::Int(1), row![Col::int(1)]),
                (Col::Int(2), row![Col::int(2)]),
                (Col::Int(3), row![Col::int(3)]),
                (Col::Int(4), row![Col::int(4)]),
                (Col::Int(5), row![Col::int(5)]),
                (Col::Int(6), row![Col::int(6)]),
            ],
        };
        let buffer: Vec<u8> = leaf.clone().try_into().unwrap();
        let restored: Page = buffer.try_into().unwrap();
        assert_eq!(restored, leaf);
    }

    #[test]
    fn leaf_size() {
        let leaf_values = vec![(Col::Int(1), row![Col::Int(10)])];
        assert_eq!(18, Page::leaf_size(&leaf_values));
    }

    #[test]
    fn node_size() {
        let node_values = vec![(Col::Int(1), 10)];
        assert_eq!(16, Page::node_size(&node_values));
    }

    #[test]
    fn insert_key_value_test() {
        let mut offsets = vec![(Col::int(0), 1), (Col::Int(237), 2)];
        insert_key_value(&mut offsets, (Col::Int(325), 3));
        assert_eq!(
            vec![(Col::Int(0), 1), (Col::Int(237), 2), (Col::Int(325), 3),],
            offsets,
        );
    }

    #[test]
    fn insert_key_value_in_order() {
        let mut values = vec![];
        let mut key_value = vec![];
        for i in 0..1000 {
            values.push((Col::Int(i), Col::Int(i)));
            insert_key_value(&mut key_value, (Col::Int(i), Col::Int(i)));
        }
        values.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(values, key_value);
    }

    #[test]
    fn check_key_value_size() {
        let mut key_size = MAX_KEY_VALUE_SIZE / 2;
        let mut value_size = MAX_KEY_VALUE_SIZE - key_size;
        key_size -= 1 + 2 + 2;
        value_size -= 1 + 2 + 2 + 1;
        let key = Col::varchar("", key_size as u16);
        let value = row![Col::varchar("", (value_size) as u16)];
        let mut values = Vec::new();
        insert_key_value(&mut values, (key, value));
        let size = Page::leaf_size(&values);
        assert_eq!(PAGE_SIZE, size);
    }

    #[test]
    fn split_huge_leaf() {
        let mut values = vec![];
        for i in 0..100 {
            values.push((Col::varchar("", 12), row![Col::int(i)]));
        }
        assert!(Page::leaf_size(&values) < PAGE_SIZE);
        values.push((Col::varchar("", 3000), row![Col::int(0)]));
        let (left, right) = split_leaf(values);
        assert!(Page::leaf_size(&left) < PAGE_SIZE);
        assert!(Page::leaf_size(&right) < PAGE_SIZE);
    }

    #[test]
    fn split_huge_node() {
        let mut values = Vec::<(Col, Offset)>::new();
        for i in 0..100 {
            values.push((Col::varchar("", 12), i));
        }
        assert!(Page::node_size(&values) < PAGE_SIZE);
        values.push((Col::varchar("", 3000), 0));
        let (left, right) = split_node(values);
        assert!(Page::node_size(&left) < PAGE_SIZE);
        assert!(Page::node_size(&right) < PAGE_SIZE);
    }
}
