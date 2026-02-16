use std::sync::mpsc::{Receiver, Sender};

use common::error::DbError;
use engine::{Engine, exec_result::ExecResult};

use crate::config::Config;

mod config;

pub struct Runner {
    engine: Engine,
    tx: Sender<Result<ExecResult, DbError>>,
    rx: Receiver<String>,
}

impl Runner {
    pub fn new(
        config: Config,
        tx: Sender<Result<ExecResult, DbError>>,
        rx: Receiver<String>,
    ) -> Result<Self, DbError> {
        let engine = Engine::new(&config.path)?;
        Ok(Self { engine, tx, rx })
    }

    pub fn run(self) -> Result<(), DbError> {
        loop {
            match self.rx.recv() {
                Ok(query) => self.execute(query)?,
                Err(err) => return Err(DbError::IO(err.to_string())),
            }
        }
    }

    fn execute(&self, query: String) -> Result<(), DbError> {
        let result = match parser::parse(&query) {
            Ok(command) => self.engine.execute(command),
            Err(err) => Err(err),
        };
        if let Err(err) = self.tx.send(result) {
            return Err(DbError::IO(err.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::mpsc, thread::spawn};

    use super::*;

    #[test]
    fn queries() {
        let (r_tx, r_rx) = mpsc::channel();
        let (q_tx, q_rx) = mpsc::channel();

        let temp_dir = tempfile::tempdir().unwrap();
        let config = Config {
            path: PathBuf::from(temp_dir.path()),
        };

        let runner = Runner::new(config, r_tx, q_rx).unwrap();
        spawn(move || {
            runner.run().unwrap();
        });

        q_tx.send("CREATE TABLE users(id INT, name VARCHAR(16))".to_string())
            .unwrap();
        let Ok(result) = r_rx.recv().unwrap() else {
            panic!("cannot get result");
        };
        assert_eq!(result.field_names.first().unwrap(), "created");
        q_tx.send("INSERT INTO users(id, name) VALUES(1, 'John')".to_string())
            .unwrap();
    }
}

