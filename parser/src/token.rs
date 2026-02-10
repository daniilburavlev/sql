use std::fmt::Display;

use common::error::DbError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token {
    Create,
    Table,
    From,
    Select,
    Insert,
    Into,
    Delete,
    Where,
    Values,
    Delimiter(char),
    Element(String),
}

impl Token {
    #[cfg(test)]
    pub(crate) fn element(e: &str) -> Self {
        Self::Element(e.to_string())
    }

    fn parse(token: &str) -> Option<Self> {
        match token {
            "create" => Some(Self::Create),
            "table" => Some(Self::Table),
            "into" => Some(Self::Into),
            "insert" => Some(Self::Insert),
            "select" => Some(Self::Select),
            "delete" => Some(Self::Delete),
            "from" => Some(Self::From),
            "where" => Some(Self::Where),
            "values" => Some(Self::Values),
            _ => None,
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create => write!(f, "CREATE"),
            Self::Table => write!(f, "TABLE"),
            Self::From => write!(f, "FROM"),
            Self::Select => write!(f, "SELECT"),
            Self::Insert => write!(f, "INSERT"),
            Self::Into => write!(f, "INSERT"),
            Self::Delete => write!(f, "DELETE"),
            Self::Where => write!(f, "WHERE"),
            Self::Values => write!(f, "VALUES"),
            Self::Delimiter(c) => write!(f, "{}", c),
            Self::Element(el) => write!(f, "'{}'", el),
        }
    }
}

pub(crate) fn tokenize(query: &str) -> Result<Vec<Token>, DbError> {
    let mut str_char = None::<char>;
    let mut tokens = Vec::new();
    let last_idx = query.len() - 1;
    let mut token_chars = Vec::new();
    let mut prev_char = '0';

    for (i, c) in query.char_indices() {
        if is_str_token(c) || str_char.is_some() {
            if str_char == Some(c) && prev_char != '\\' {
                let token: String = token_chars.into_iter().collect();
                token_chars = Vec::new();
                tokens.push(Token::Element(token));
                str_char = None;
                continue;
            } else if last_idx == i && str_char.is_some() {
                return Err(DbError::EOF(format!(
                    "uexpected close tag: {}",
                    str_char.unwrap()
                )));
            } else if str_char.is_none() {
                str_char = Some(c);
            } else {
                token_chars.push(c);
            }
        } else if is_delimeter(c) || i == last_idx {
            if last_idx == i && !is_delimeter(c) {
                token_chars.push(c);
            }
            if !token_chars.is_empty() {
                let token: String = token_chars.into_iter().collect();
                if let Some(token) = Token::parse(&token.to_lowercase()) {
                    tokens.push(token);
                } else {
                    tokens.push(Token::Element(token));
                }
            }
            if is_markable_delimeter(c) {
                tokens.push(Token::Delimiter(c));
            }
            token_chars = Vec::new();
        } else {
            token_chars.push(c);
        }
        prev_char = c;
    }
    Ok(tokens)
}

fn is_str_token(c: char) -> bool {
    c == '\'' || c == '"'
}

fn is_markable_delimeter(c: char) -> bool {
    c == '(' || c == ')' || c == ','
}

fn is_delimeter(c: char) -> bool {
    c == ' ' || c == '\n' || is_markable_delimeter(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        let query = "CREATE TABLE(id int, name varchar(256))";
        let tokens = tokenize(query).unwrap();
        assert_eq!(
            vec![
                Token::Create,
                Token::Table,
                Token::Delimiter('('),
                Token::element("id"),
                Token::element("int"),
                Token::Delimiter(','),
                Token::element("name"),
                Token::element("varchar"),
                Token::Delimiter('('),
                Token::element("256"),
                Token::Delimiter(')'),
                Token::Delimiter(')'),
            ],
            tokens
        );
    }

    #[test]
    fn select() {
        let query = "SELECT *, id, name FROM test WHERE \"SELECT * FROM users\"";
        let tokens = tokenize(query).unwrap();
        assert_eq!(
            vec![
                Token::Select,
                Token::element("*"),
                Token::Delimiter(','),
                Token::element("id"),
                Token::Delimiter(','),
                Token::element("name"),
                Token::From,
                Token::element("test"),
                Token::Where,
                Token::element("SELECT * FROM users"),
            ],
            tokens
        );
    }

    #[test]
    fn insert() {
        let query = "INSERT INTO test(id, name) VALUES(1, 'John'), (2, 'Mary')";
        let tokens = tokenize(query).unwrap();
        assert_eq!(
            vec![
                Token::Insert,
                Token::Into,
                Token::element("test"),
                Token::Delimiter('('),
                Token::element("id"),
                Token::Delimiter(','),
                Token::element("name"),
                Token::Delimiter(')'),
                Token::Values,
                Token::Delimiter('('),
                Token::element("1"),
                Token::Delimiter(','),
                Token::element("John"),
                Token::Delimiter(')'),
                Token::Delimiter(','),
                Token::Delimiter('('),
                Token::element("2"),
                Token::Delimiter(','),
                Token::element("Mary"),
                Token::Delimiter(')'),
            ],
            tokens
        );
    }

    #[test]
    fn str_with_escaped() {
        let query = "\"\\\" \"";
        assert_eq!(vec![Token::element("\\\" ")], tokenize(query).unwrap());

        let query = "\"\\'\"";
        assert_eq!(vec![Token::element("\\'")], tokenize(query).unwrap());
    }

    #[test]
    fn not_closed_str() {
        let query = "\"test some string";
        assert!(tokenize(query).is_err());
    }

    #[test]
    fn check_string() {
        assert!(is_str_token('"'));
        assert!(is_str_token('\''));
    }

    #[test]
    fn display() {
        assert_eq!("CREATE", Token::Create.to_string());
        assert_eq!("INSERT", Token::Insert.to_string());
        assert_eq!("TABLE", Token::Table.to_string());
        assert_eq!("SELECT", Token::Select.to_string());
        assert_eq!("WHERE", Token::Where.to_string());
    }
}
