use crate::col::Col;
use std::collections::HashMap;

pub struct Row<'a> {
    pub col_names: Vec<&'a str>,
    pub columns: HashMap<&'a str, Col>,
}
