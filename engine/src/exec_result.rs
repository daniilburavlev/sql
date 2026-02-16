use row::Col;

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok() {
        let exec_result = ExecResult::ok("created", 1);
        assert_eq!("created", exec_result.field_names.first().unwrap());
        assert_eq!(
            Col::int(1),
            *exec_result.fields.first().unwrap().first().unwrap()
        );
    }
}
