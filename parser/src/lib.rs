mod command;
mod token;

pub use command::Command;
use common::error::DbError;

pub fn parse(query: &str) -> Result<Command, DbError> {
    let tokens = token::tokenize(query)?;
    Command::parse(tokens)
}
