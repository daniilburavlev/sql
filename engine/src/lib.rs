use std::{collections::HashMap, fs, path::Path};

use common::error::DbError;
use parser::Command;
use row::{Col, ColType, Row, RowType};

use crate::{exec_result::ExecResult, storage::Storage};

pub mod exec_result;
mod storage;

pub struct Engine {
    storage: Storage,
}

impl Engine {
    pub fn new(dir: &Path) -> Result<Self, DbError> {
        fs::create_dir_all(dir)?;
        let storage = Storage::new(dir)?;
        Ok(Self { storage })
    }

    pub fn execute(&self, command: Command) -> Result<ExecResult, DbError> {
        match command {
            Command::Create { name, fields } => {
                let created = self.execute_create(&name, fields)?;
                Ok(ExecResult::ok("created", created as i32))
            }
            Command::Insert {
                table,
                fields,
                values,
            } => {
                let inserted = self.execute_insert(&table, fields, values)?;
                Ok(ExecResult::ok("inserted", inserted as i32))
            }
            Command::Select { table, fields } => {
                let rows = self.execute_select(&table, fields.clone())?;
                Ok(ExecResult {
                    field_names: fields,
                    fields: rows,
                })
            }
        }
    }

    fn execute_create(&self, name: &str, columns: Vec<ColType>) -> Result<usize, DbError> {
        let row_type = RowType { columns };
        self.storage.create(name, row_type)
    }

    fn execute_insert(
        &self,
        name: &str,
        fields: Vec<String>,
        values: Vec<Vec<String>>,
    ) -> Result<usize, DbError> {
        let row_type = self.storage.get_row_type(name)?;
        let rows = build_rows(name, row_type, fields, values)?;
        let rows: Vec<(Col, Row)> = rows
            .into_iter()
            .map(|columns| (columns.first().cloned().unwrap(), Row { columns }))
            .collect();
        self.storage.insert(name, rows)
    }

    fn execute_select(&self, name: &str, fields: Vec<String>) -> Result<Vec<Vec<Col>>, DbError> {
        let fields_len = fields.len();
        let row_type = self.storage.get_row_type(name)?;
        let indexes = get_indexes(name, row_type, fields)?;
        let raw_rows = self.storage.select_all(name)?;
        let mut rows = Vec::with_capacity(raw_rows.len());
        for raw_row in raw_rows {
            let mut row = Vec::with_capacity(fields_len);
            for i in indexes.iter() {
                let col = raw_row.columns.get(*i).cloned().unwrap();
                row.push(col);
            }
            rows.push(row);
        }
        Ok(rows)
    }
}

fn get_indexes(table: &str, row_type: RowType, fields: Vec<String>) -> Result<Vec<usize>, DbError> {
    let mut indexes_by_names: HashMap<&str, usize> = HashMap::new();
    for (i, col_type) in row_type.columns.iter().enumerate() {
        indexes_by_names.insert(col_type.get_name(), i);
    }
    let mut indexes = Vec::with_capacity(fields.len());
    for field in fields {
        let Some(i) = indexes_by_names.get(field.as_str()) else {
            return Err(DbError::field_not_found(&field, table));
        };
        indexes.push(*i);
    }
    Ok(indexes)
}

fn check_primary_key(row_type: &RowType, fields: &[String]) -> Result<(), DbError> {
    let pk = row_type.get_primary_key()?;
    let name = pk.get_name();
    for field in fields.iter() {
        if field == name {
            return Ok(());
        }
    }
    Err(DbError::PrimaryKeyNotSet)
}

fn build_rows(
    table: &str,
    row_type: RowType,
    fields: Vec<String>,
    values: Vec<Vec<String>>,
) -> Result<Vec<Vec<Col>>, DbError> {
    let fields_len = fields.len();
    let mut rows = Vec::new();
    check_primary_key(&row_type, &fields)?;
    for group in values {
        if fields_len != group.len() {
            return Err(DbError::invalid_input("wrong amount of insert values"));
        }
        let mut row = HashMap::new();
        for (i, value) in group.into_iter().enumerate() {
            let name = fields.get(i).cloned().unwrap();
            row.insert(name, value);
        }
        let row = build_row(table, &row_type, row)?;
        rows.push(row);
    }
    Ok(rows)
}

fn build_row(
    table: &str,
    row_type: &RowType,
    mut values: HashMap<String, String>,
) -> Result<Vec<Col>, DbError> {
    let mut cols = Vec::new();
    for col_type in row_type.columns.iter() {
        let name = col_type.get_name();
        match col_type {
            ColType::Int(_) => {
                let value = values.remove(name).unwrap_or(String::from("0"));
                let value: i32 = value.parse()?;
                cols.push(Col::Int(value));
            }
            ColType::BigInt(_) => {
                let value = values.remove(name).unwrap_or(String::from("0"));
                let value: i64 = value.parse()?;
                cols.push(Col::BigInt(value));
            }
            ColType::Varchar(_, size) => {
                let value = values.remove(name).unwrap_or_default();
                cols.push(Col::Varchar(value, *size));
            }
        }
    }
    if let Some(key) = values.into_keys().next() {
        return Err(DbError::field_not_found(&key, table));
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        let temp_dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(temp_dir.path()).unwrap();
        engine
            .execute(Command::Create {
                name: "test".to_string(),
                fields: vec![ColType::int("id")],
            })
            .unwrap();
        engine
            .execute(Command::Insert {
                table: "test".to_string(),
                fields: vec!["id".to_string()],
                values: vec![vec![1.to_string()], vec![2.to_string()]],
            })
            .unwrap();
        let rows = engine
            .execute(Command::Select {
                fields: vec!["id".to_string()],
                table: "test".to_string(),
            })
            .unwrap();
        assert_eq!(
            rows,
            ExecResult {
                field_names: vec!["id".to_string()],
                fields: vec![vec![Col::int(1)], vec![Col::int(2)]]
            }
        );
    }
}
