mod command;
mod token;

pub use command::Command;
use common::error::DbError;

pub fn parse(query: &str) -> Result<Command, DbError> {
    let tokens = token::tokenize(query)?;
    Command::parse(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_insert() {
        let command = parse("INSERT INTO users(id, name) VALUES(10, 'Daniil')").unwrap();
        assert_eq!(
            Command::Insert {
                table: "users".to_string(),
                fields: vec!["id".to_string(), "name".to_string()],
                values: vec![vec!["10".to_string(), "Daniil".to_string()]]
            },
            command
        );
    }

    #[test]
    fn parse_select_with_no_fields() {
        let query = "SELECT FROM users";
        let command = parse(query).unwrap();
        assert_eq!(
            Command::Select {
                table: "users".to_string(),
                fields: vec![],
            },
            command
        );
    }
}
