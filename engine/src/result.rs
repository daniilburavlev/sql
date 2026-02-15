use std::collections::HashMap;

use row::Col;

pub struct ExecResult {
    pub field_names: Vec<String>,
    pub fields: HashMap<u32, Vec<Col>>,
}

impl ExecResult {
    pub fn ok(header: &str, count: i32) -> Self {
        let mut fields = HashMap::new();
        fields.insert(1, vec![Col::int(count)]);
        Self {
            field_names: vec![header.to_string()],
            fields,
        }
    }
}
