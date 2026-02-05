use crate::error::DbError;

const TYPE_SIZE: usize = 1;
pub const PAGE_SIZE: usize = 4 * 1024;
pub const LEN_SIZE: usize = 4;
pub const PTR_SIZE: usize = 4;

pub type Offset = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Page {
    Node {
        parent: u32,
        children: Vec<(String, Offset)>,
    },
    Leaf {
        parent: u32,
        values: Vec<(String, String)>,
    },
}

impl Page {
    pub fn page_type(&self) -> u8 {
        match self {
            Self::Node { .. } => 1,
            Self::Leaf { .. } => 2,
        }
    }

    pub fn leaf_size(values: &Vec<(String, String)>) -> usize {
        let mut size = TYPE_SIZE + PTR_SIZE + LEN_SIZE;
        for (k, v) in values {
            let k_len = k.len();
            size += k_len;
            size += LEN_SIZE;
            let v_len = v.len();
            size += v_len;
            size += LEN_SIZE;
        }
        size
    }

    pub fn node_size(values: &Vec<(String, u32)>) -> usize {
        let mut size = TYPE_SIZE + PTR_SIZE + LEN_SIZE;
        for (key, _) in values {
            let key_len = key.len();
            size += key_len;
            size += LEN_SIZE;
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
        let mut parent = [0u8; PTR_SIZE];
        parent.copy_from_slice(&buffer[offset..offset + PTR_SIZE]);
        let parent = u32::from_be_bytes(parent);
        offset += PTR_SIZE;

        let mut elements = [0u8; LEN_SIZE];
        elements.copy_from_slice(&buffer[offset..offset + LEN_SIZE]);
        let elements = u32::from_be_bytes(elements);

        match page_type {
            1 => {
                let mut children = Vec::new();
                let mut offset = PAGE_SIZE - LEN_SIZE;
                for _ in 0..elements {
                    let mut key_len = [0u8; LEN_SIZE];
                    key_len.copy_from_slice(&buffer[offset..offset + LEN_SIZE]);
                    let key_len = u32::from_be_bytes(key_len) as usize;
                    offset -= key_len;
                    if key_len == 0 {
                        break;
                    }
                    let mut key = vec![0u8; key_len];
                    key.copy_from_slice(&buffer[offset..offset + key_len]);
                    let key = String::from_utf8_lossy(&key);
                    offset -= PTR_SIZE;

                    let mut pointer = [0u8; PTR_SIZE];
                    pointer.copy_from_slice(&buffer[offset..offset + PTR_SIZE]);
                    let pointer = u32::from_be_bytes(pointer);
                    offset -= LEN_SIZE;
                    children.push((key.to_string(), pointer));
                }
                Ok(Self::Node { parent, children })
            }
            2 => {
                let mut values = Vec::new();
                let mut offset = PAGE_SIZE - LEN_SIZE;
                for _ in 0..elements {
                    let mut key_len = [0u8; LEN_SIZE];
                    key_len.copy_from_slice(&buffer[offset..offset + LEN_SIZE]);
                    let key_len = u32::from_be_bytes(key_len) as usize;
                    offset -= key_len;
                    if key_len == 0 {
                        break;
                    }
                    let mut key = vec![0u8; key_len];
                    key.copy_from_slice(&buffer[offset..offset + key_len]);
                    let key = String::from_utf8_lossy(&key);
                    offset -= PTR_SIZE;

                    let mut value_len = [0u8; LEN_SIZE];
                    value_len.copy_from_slice(&buffer[offset..offset + LEN_SIZE]);
                    let value_len = u32::from_be_bytes(value_len) as usize;
                    offset -= value_len;
                    if value_len == 0 {
                        break;
                    }
                    let mut value = vec![0u8; value_len];
                    value.copy_from_slice(&buffer[offset..offset + value_len]);
                    let value = String::from_utf8_lossy(&value);
                    offset -= PTR_SIZE;
                    values.push((key.to_string(), value.to_string()));
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
        let page_type = self.page_type();
        buffer[0] = page_type;

        match self {
            Self::Node { parent, children } => {
                if Self::node_size(&children) > PAGE_SIZE {
                    return Err(DbError::Encoding);
                }
                let mut offset = TYPE_SIZE;
                buffer[offset..offset + PTR_SIZE].copy_from_slice(&parent.to_be_bytes());
                offset += PTR_SIZE;
                buffer[offset..offset + LEN_SIZE]
                    .copy_from_slice(&(children.len() as u32).to_be_bytes());

                let mut offset = PAGE_SIZE - LEN_SIZE;
                for (key, pointer) in children {
                    let raw_key = key.as_bytes();
                    let key_len = raw_key.len();
                    buffer[offset..offset + LEN_SIZE]
                        .copy_from_slice(&(key_len as u32).to_be_bytes());
                    offset -= key_len;

                    buffer[offset..offset + key_len].copy_from_slice(raw_key);
                    offset -= PTR_SIZE;

                    buffer[offset..offset + PTR_SIZE].copy_from_slice(&pointer.to_be_bytes());
                    offset -= PTR_SIZE;
                }
            }
            Self::Leaf { parent, values } => {
                if Self::leaf_size(&values) > PAGE_SIZE {
                    return Err(DbError::Encoding);
                }
                let mut offset = TYPE_SIZE;
                buffer[offset..offset + PTR_SIZE].copy_from_slice(&parent.to_be_bytes());
                offset += PTR_SIZE;
                buffer[offset..offset + LEN_SIZE]
                    .copy_from_slice(&(values.len() as u32).to_be_bytes());

                let mut offset = PAGE_SIZE - LEN_SIZE;
                for (key, value) in values {
                    let raw_key = key.as_bytes();
                    let key_len = raw_key.len();
                    buffer[offset..offset + LEN_SIZE]
                        .copy_from_slice(&(key_len as u32).to_be_bytes());
                    offset -= key_len;

                    buffer[offset..offset + key_len].copy_from_slice(raw_key);
                    offset -= PTR_SIZE;

                    let raw_value = value.as_bytes();
                    let value_len = raw_value.len();
                    buffer[offset..offset + LEN_SIZE]
                        .copy_from_slice(&(value_len as u32).to_be_bytes());
                    offset -= value_len;

                    buffer[offset..offset + value_len].copy_from_slice(raw_value);
                    if offset > PTR_SIZE {
                        offset -= PTR_SIZE;
                    }
                }
            }
        }
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_node_convert() {
        let node = Page::Node {
            parent: 1337,
            children: vec![
                ("1".to_string(), 1),
                ("2".to_string(), 2),
                ("3".to_string(), 3),
                ("4".to_string(), 4),
                ("5".to_string(), 5),
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
                ("1".to_string(), "1233".to_string()),
                ("23".to_string(), "23".to_string()),
                ("3321".to_string(), "311111j".to_string()),
                ("42".to_string(), "431".to_string()),
                ("51321".to_string(), "5".to_string()),
            ],
        };
        let buffer: Vec<u8> = leaf.clone().try_into().unwrap();
        let restored: Page = buffer.try_into().unwrap();
        assert_eq!(restored, leaf);
    }

    #[test]
    fn leaf_size() {
        let leaf_values = vec![(1.to_string(), 123.to_string())];
        assert_eq!(21, Page::leaf_size(&leaf_values));
    }

    #[test]
    fn node_size() {
        let node_values = vec![(1.to_string(), 123)];
        assert_eq!(18, Page::node_size(&node_values));
    }
}
