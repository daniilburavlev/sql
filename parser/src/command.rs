use core::fmt;
use std::str::FromStr;

use common::error::DbError;
use row::ColType;

use crate::token::Token;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Command {
    Create {
        name: String,
        fields: Vec<ColType>,
    },
    Insert {
        table: String,
        fields: Vec<String>,
        values: Vec<Vec<String>>,
    },
    Select {
        fields: Vec<String>,
        table: String,
    },
}

impl Command {
    pub(crate) fn parse(tokens: Vec<Token>) -> Result<Command, DbError> {
        if tokens.is_empty() {
            return Err(DbError::invalid_input("empty input"));
        }
        let idx = 1;
        match tokens.first().unwrap() {
            Token::Create => Self::parse_create(tokens, idx),
            Token::Insert => Self::parse_insert(tokens, idx),
            Token::Select => Self::parse_select(tokens, idx),
            other => Err(DbError::InvalidInput(format!(
                "unexpected symbol: {}",
                other
            ))),
        }
    }

    fn parse_create(tokens: Vec<Token>, mut idx: usize) -> Result<Command, DbError> {
        match tokens.get(idx) {
            Some(Token::Table) => {}
            Some(token) => {
                return Err(DbError::InvalidInput(format!(
                    "unexpected symbol: {}",
                    token
                )));
            }
            None => return Err(DbError::eof("expected 'CREATE' specifier")),
        }
        idx += 1;
        let name = match tokens.get(idx) {
            Some(Token::Element(name)) => name.clone(),
            Some(token) => {
                return Err(DbError::InvalidInput(format!(
                    "unexpected symbol: {}",
                    token
                )));
            }
            None => return Err(DbError::eof("expected 'table_name' specifier")),
        };
        idx += 1;
        check_delimeter(tokens.get(idx), '(')?;
        idx += 1;
        let mut fields = vec![];
        let len = tokens.len();
        let Some(Token::Delimiter(')')) = tokens.last() else {
            return Err(DbError::invalid_input("expect: ')'"));
        };
        while idx < len - 1 {
            let Some(Token::Element(field_name)) = tokens.get(idx) else {
                return Err(DbError::invalid_input("expected column name"));
            };
            idx += 1;
            let Some(Token::Element(field_type)) = tokens.get(idx) else {
                return Err(DbError::invalid_input("expected column type specifier"));
            };
            idx += 1;
            let field = match field_type.to_lowercase().as_str() {
                "int" => ColType::Int(field_name.clone()),
                "bigint" => ColType::BigInt(field_name.clone()),
                "varchar" => {
                    check_delimeter(tokens.get(idx), '(')?;
                    idx += 1;
                    let size: u16 = get_num(tokens.get(idx))?;
                    idx += 1;
                    check_delimeter(tokens.get(idx), ')')?;
                    idx += 1;
                    ColType::Varchar(field_name.clone(), size)
                }
                _ => {
                    return Err(DbError::InvalidInput(format!(
                        "unknown column type: {}",
                        field_type
                    )));
                }
            };
            fields.push(field);
            idx += 1;
        }
        Ok(Self::Create { name, fields })
    }

    fn parse_insert(tokens: Vec<Token>, mut idx: usize) -> Result<Command, DbError> {
        let Some(Token::Into) = tokens.get(idx) else {
            return Err(DbError::invalid_input("expected INTO"));
        };
        idx += 1;
        let Some(Token::Element(table_name)) = tokens.get(idx) else {
            return Err(DbError::invalid_input("expected 'table_name' specifier'"));
        };
        idx += 1;
        check_delimeter(tokens.get(idx), '(')?;
        idx += 1;
        let len = tokens.len();
        let mut fields = vec![];
        while idx < len {
            match tokens.get(idx) {
                Some(Token::Element(field)) => {
                    idx += 1;
                    fields.push(field.clone());
                    let delimiter = tokens.get(idx);
                    idx += 1;
                    if delimiter.is_none() {
                        return Err(DbError::eof(""));
                    }
                    let is_full = check_delimeter(delimiter, ')');
                    if check_delimeter(delimiter, ',').is_err() && is_full.is_err() {
                        return Err(DbError::InvalidInput(format!(
                            "expect: ',', found: {}",
                            delimiter.unwrap()
                        )));
                    }
                    if is_full.is_ok() {
                        break;
                    }
                }
                Some(token) => return Err(DbError::InvalidInput(format!("unexpected: {}", token))),
                _ => return Err(DbError::eof("")),
            }
        }
        let Some(Token::Values) = tokens.get(idx) else {
            return Err(DbError::invalid_input("expect VALUES"));
        };
        idx += 1;
        let fields_len = fields.len();
        let mut values = Vec::new();
        let mut sub_values = Vec::with_capacity(fields_len);
        while idx < len {
            check_delimeter(tokens.get(idx), '(')?;
            idx += 1;
            let limit = idx + fields_len * 2 - 1;
            while idx < limit {
                match tokens.get(idx) {
                    Some(Token::Element(value)) => {
                        sub_values.push(value.clone());
                    }
                    Some(token) => {
                        return Err(DbError::InvalidInput(format!(
                            "unexpected token: {}",
                            token
                        )));
                    }
                    None => return Err(DbError::eof("")),
                }
                idx += 1;
                if idx < limit - 1 {
                    check_delimeter(tokens.get(idx), ',')?;
                    idx += 1;
                }
            }
            check_delimeter(tokens.get(idx), ')')?;
            idx += 1;
            values.push(sub_values);
            sub_values = Vec::with_capacity(fields_len);
        }
        Ok(Self::Insert {
            table: table_name.clone(),
            fields,
            values,
        })
    }

    fn parse_select(tokens: Vec<Token>, mut idx: usize) -> Result<Command, DbError> {
        let mut fields = Vec::new();
        let len = tokens.len();
        let mut token = None::<Token>;
        for i in idx..len {
            match tokens.get(i) {
                Some(Token::Element(field)) => {
                    token = Some(Token::Element(field.to_string()));
                }
                Some(Token::Delimiter(',')) => match token {
                    Some(Token::Element(field)) => {
                        fields.push(field);
                        token = Some(Token::Delimiter(','));
                    }
                    Some(token) => {
                        return Err(DbError::InvalidInput(format!(
                            "unexpected token: {}",
                            token
                        )));
                    }
                    None => return Err(DbError::invalid_input("expected field specifier")),
                },
                Some(Token::From) => match token {
                    Some(Token::Element(field)) => {
                        fields.push(field);
                        idx = i + 1;
                        break;
                    }
                    Some(Token::Delimiter(',')) => {
                        return Err(DbError::invalid_input("unexpected token ','"));
                    }
                    Some(token) => {
                        return Err(DbError::InvalidInput(format!(
                            "unexpected token: {}",
                            token
                        )));
                    }
                    None => {}
                },
                Some(token) => return Err(DbError::InvalidInput(format!("invalid: {}", token))),
                None => return Err(DbError::eof("expected fields")),
            }
            if i == len - 1 {
                return Err(DbError::invalid_input("mission FROM clause"));
            }
        }
        let Some(Token::Element(table)) = tokens.get(idx) else {
            return Err(DbError::invalid_input("missing FROM specifier"));
        };
        Ok(Self::Select {
            fields,
            table: table.to_string(),
        })
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create { name, fields } => {
                write!(f, "CREATE TABLE {}(", name)?;
                let len = fields.len();
                for (i, field) in fields.iter().enumerate() {
                    write!(f, "{}", field)?;
                    if i < len - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")?;
            }
            Self::Insert {
                table,
                fields,
                values,
            } => {
                write!(f, "INSERT INTO {}(", table)?;
                let len = fields.len();
                for (i, field) in fields.iter().enumerate() {
                    write!(f, "{}", field)?;
                    if i < len - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ") VALUES")?;
                let len = values.len();
                for (i, group) in values.iter().enumerate() {
                    write!(f, "(")?;
                    let group_len = group.len();
                    for (i, value) in group.iter().enumerate() {
                        write!(f, "'{}'", value)?;
                        if i < group_len - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ")")?;
                    if i < len - 1 {
                        write!(f, ", ")?;
                    }
                }
            }
            Self::Select { table, fields } => {
                write!(f, "SELECT ")?;
                let len = fields.len();
                for (i, field) in fields.iter().enumerate() {
                    write!(f, "{}", field)?;
                    if i < len - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, " FROM {}", table)?;
            }
        }
        Ok(())
    }
}

fn get_num<T: FromStr>(token: Option<&Token>) -> Result<T, DbError> {
    match token {
        Some(Token::Element(num)) => num
            .as_str()
            .parse()
            .map_err(|_| DbError::InvalidInput(format!("expected int, found: '{}'", num))),
        Some(token) => Err(DbError::InvalidInput(format!("unexpected: {}", token))),
        None => Err(DbError::eof("expected int value")),
    }
}

fn check_delimeter(token: Option<&Token>, ch: char) -> Result<(), DbError> {
    let Some(Token::Delimiter(c)) = token else {
        return Err(DbError::InvalidInput(format!("expected: '{}'", ch)));
    };
    if *c != ch {
        return Err(DbError::InvalidInput(format!(
            "expected: '{}', found: '{}'",
            ch, c
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_command() {
        if let Err(DbError::InvalidInput(err)) = Command::parse(vec![]) {
            assert_eq!("empty input", err);
        } else {
            panic!("empty not validated");
        }
    }

    #[test]
    fn create() {
        let tokens = vec![
            Token::Create,
            Token::Table,
            Token::element("users"),
            Token::Delimiter('('),
            Token::element("id"),
            Token::element("int"),
            Token::Delimiter(','),
            Token::element("name"),
            Token::element("varchar"),
            Token::Delimiter('('),
            Token::element("10"),
            Token::Delimiter(')'),
            Token::Delimiter(')'),
        ];
        let command = Command::parse(tokens).unwrap();
        assert_eq!(
            Command::Create {
                name: "users".to_string(),
                fields: vec![
                    ColType::Int("id".to_string()),
                    ColType::Varchar("name".to_string(), 10)
                ]
            },
            command
        );
    }

    #[test]
    fn miss_table() {
        let tokens = vec![Token::Create];
        let Err(DbError::EOF(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("expected 'CREATE' specifier", err);

        let tokens = vec![Token::Create, Token::element("table")];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("unexpected symbol: 'table'", err);
    }

    #[test]
    fn table_name() {
        let tokens = vec![Token::Create, Token::Table];
        let Err(DbError::EOF(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("expected 'table_name' specifier", err);

        let tokens = vec![Token::Create, Token::Table, Token::Table];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("unexpected symbol: TABLE", err);
    }

    #[test]
    fn illegal_command() {
        let tokens = vec![Token::Where];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("unexpected symbol: WHERE", err);
    }

    #[test]
    fn not_closed_create() {
        let tokens = vec![
            Token::Create,
            Token::Table,
            Token::element("users"),
            Token::Delimiter('('),
            Token::element("id"),
            Token::element("int"),
        ];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("expect: ')'", err);
    }

    #[test]
    fn invalid_type() {
        let tokens = vec![
            Token::Create,
            Token::Table,
            Token::element("users"),
            Token::Delimiter('('),
            Token::element("id"),
            Token::element("fda"),
            Token::Delimiter(')'),
        ];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("unknown column type: fda", err);
    }

    #[test]
    fn invalid_column() {
        let tokens = vec![
            Token::Create,
            Token::Table,
            Token::element("users"),
            Token::Delimiter('('),
            Token::Create,
            Token::Create,
            Token::Delimiter(')'),
        ];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("expected column name", err);
    }

    #[test]
    fn invalid_column_type() {
        let tokens = vec![
            Token::Create,
            Token::Table,
            Token::element("users"),
            Token::Delimiter('('),
            Token::element("id"),
            Token::Create,
            Token::Delimiter(')'),
        ];
        let Err(DbError::InvalidInput(err)) = Command::parse(tokens) else {
            panic!("error not validated");
        };
        assert_eq!("expected column type specifier", err);
    }

    #[test]
    fn num_from_el() {
        let Err(DbError::InvalidInput(err)) = get_num::<u16>(Some(&Token::element("i1231fdsaf")))
        else {
            panic!("error nov validated");
        };
        assert_eq!("expected int, found: 'i1231fdsaf'", err);

        let Err(DbError::InvalidInput(err)) = get_num::<u16>(Some(&Token::Create)) else {
            panic!("error nov validated");
        };
        assert_eq!("unexpected: CREATE", err);
    }

    #[test]
    fn invalid_delimiter() {
        let Err(DbError::InvalidInput(err)) = check_delimeter(Some(&Token::Create), ' ') else {
            panic!("error not valdated");
        };
        assert_eq!("expected: ' '", err);

        let Err(DbError::InvalidInput(err)) = check_delimeter(Some(&Token::Delimiter('a')), ' ')
        else {
            panic!("error not valdated");
        };
        assert_eq!("expected: ' ', found: 'a'", err);
    }

    #[test]
    fn insert() {
        let tokens = vec![
            Token::Insert,
            Token::Into,
            Token::element("users"),
            Token::Delimiter('('),
            Token::element("id"),
            Token::Delimiter(','),
            Token::element("name"),
            Token::Delimiter(')'),
            Token::Values,
            Token::Delimiter('('),
            Token::element("10"),
            Token::Delimiter(','),
            Token::element("Lucie"),
            Token::Delimiter(')'),
        ];
        let command = Command::parse_insert(tokens, 1).unwrap();
        println!("{:?}", command);
    }

    #[test]
    fn select() {
        let query = vec![
            Token::Select,
            Token::element("*"),
            Token::Delimiter(','),
            Token::element("name"),
            Token::From,
            Token::element("users"),
        ];
        let command = Command::parse(query).unwrap();
        assert_eq!(
            Command::Select {
                fields: vec!["*".to_string(), "name".to_string()],
                table: "users".to_string(),
            },
            command
        );
    }

    #[test]
    fn select_unexpected_delimiter() {
        let query = vec![
            Token::Select,
            Token::Delimiter(','),
            Token::From,
            Token::element("users"),
        ];
        let Err(DbError::InvalidInput(err)) = Command::parse(query) else {
            panic!("syntax error is not validated");
        };
        assert_eq!("expected field specifier", err);
    }

    #[test]
    fn display_select() {
        let select = Command::Select {
            fields: vec!["*".to_string()],
            table: "users".to_string(),
        };
        assert_eq!(select.to_string(), "SELECT * FROM users");
    }

    #[test]
    fn display_create() {
        let select = Command::Create {
            name: "users".to_string(),
            fields: vec![ColType::int("id"), ColType::varchar("name", 16)],
        };
        assert_eq!(
            select.to_string(),
            "CREATE TABLE users(id INT, name VARCHAR(16))"
        );
    }

    #[test]
    fn display_insert() {
        let select = Command::Insert {
            table: "users".to_string(),
            fields: vec!["id".to_string(), "name".to_string()],
            values: vec![vec!["1".to_string(), "John".to_string()]],
        };
        assert_eq!(
            select.to_string(),
            "INSERT INTO users(id, name) VALUES('1', 'John')"
        );
    }
}
