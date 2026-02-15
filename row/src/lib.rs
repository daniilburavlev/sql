mod col;
mod col_type;
mod row;
mod row_type;

pub use col::Col;
pub use col_type::ColType;
pub use row::Row;
pub use row_type::RowType;

#[macro_export]
macro_rules! row {
    [$cols:expr] => {
        Row { columns: vec![$cols] }
    };
}

#[macro_export]
macro_rules! row_type {
    [$cols:expr] => {
        RowType {columns: vec![$cols]}
    };
}
