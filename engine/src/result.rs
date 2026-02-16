use row::Col;

pub struct ExecResult {
    pub field_names: Vec<String>,
    pub fields: Vec<Vec<Col>>,
}

impl ExecResult {
    pub fn ok(header: &str, count: i32) -> Self {
        Self {
            field_names: vec![header.to_string()],
            fields: vec![vec![Col::int(count)]],
        }
    }
}
