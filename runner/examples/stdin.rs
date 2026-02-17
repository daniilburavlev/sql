use std::{
    io::{self, Write},
    path::PathBuf,
    process::exit,
    sync::mpsc,
    thread::spawn,
};

use common::error::DbError;
use engine::exec_result::ExecResult;
use row::Col;
use runner::{Runner, config::Config};

fn main() -> Result<(), DbError> {
    let (r_tx, r_rx) = mpsc::channel();
    let (q_tx, q_rx) = mpsc::channel();

    let config = Config::builder().path(PathBuf::from("storage")).build();

    let runner = Runner::new(config, r_tx, q_rx)?;

    spawn(move || {
        runner.run().unwrap();
    });

    loop {
        let mut input = String::new();
        print!("sql> ");
        io::stdout().flush().unwrap();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.trim_end() == "exit" {
                    break;
                }
                q_tx.send(input).expect("query channel closed");
                let result = r_rx.recv().expect("result channel closed");
                print_result(result);
            }
            Err(err) => {
                eprintln!("ERR: {}", err);
                exit(-1);
            }
        }
    }
    Ok(())
}

fn print_result(result: Result<ExecResult, DbError>) {
    match result {
        Ok(result) => {
            for field_name in result.field_names {
                print!("| {0: <10} ", field_name);
            }
            println!("|");
            for row in result.fields {
                for col in row {
                    match col {
                        Col::Int(value) => print!("| {0: <10} ", value),
                        Col::BigInt(value) => print!("| {0: <10} ", value),
                        Col::Varchar(value, _) => print!("| {0: <10} ", value),
                    }
                }
                println!("|");
            }
        }
        Err(err) => eprintln!("ERR: {}", err),
    }
}
