use std::io::{self, BufRead, Write};
use std::process::exit;

mod query;
mod storage;
mod types;

use crate::query::sql::{parse_statement, Statement};
use crate::storage::btree::{self, BTree};
use crate::types::values::*;

#[derive(Debug, Clone, Eq, PartialEq)]
enum MetaCommand {
    Exit,
}

fn parse_meta(cmd: &str) -> Option<MetaCommand> {
    match cmd {
        ".exit" => Some(MetaCommand::Exit),
        _ => None,
    }
}

fn do_meta(cmd: MetaCommand) {
    match cmd {
        MetaCommand::Exit => {
            exit(0);
        }
    }
}

fn do_select(bt: &BTree<i32, [Value; 2]>, stmt: Statement<i32, [Value; 2]>) -> Vec<Row> {
    match stmt {
        Statement::SelectAll => bt
            .find_range(&i32::MIN, &i32::MAX)
            .into_iter()
            .map(|(k, v)| vec![Value::Int(k), v[0].clone(), v[1].clone()])
            .collect(),
        Statement::SelectOne(k) => match bt.find(&k) {
            Some(vs) => {
                vec![vec![Value::Int(k), vs[0].clone(), vs[1].clone()]]
            }
            None => vec![],
        },
        _ => vec![],
    }
}

// HARDCODED TABLE
// id:       int
// username: varchar(32)
// email:    varchar(255)

// insert 1 'meiji163' 'meiji163@github.com'

fn main() -> io::Result<()> {
    let mut bt: btree::BTree<i32, [Value; 2]> = btree::BTree::new(101);

    let mut input_buf = String::with_capacity(4096);
    let mut stdin = io::stdin().lock();

    loop {
        input_buf.clear();
        print!("db> ");
        io::stdout().flush().unwrap();

        stdin.read_line(&mut input_buf)?;
        let input = input_buf.as_str().trim();
        if input.starts_with('.') {
            match parse_meta(&input) {
                Some(cmd) => do_meta(cmd),
                None => println!("error: meta command `{}` not recognized", input),
            }
        } else {
            match parse_statement(&input) {
                Some(stmt) => match stmt {
                    Statement::SelectAll | Statement::SelectOne(_) => {
                        println!("{0: <5} | {1: <32} | {2: <32}", "id", "username", "email");
                        let rows = do_select(&bt, stmt);
                        for r in rows.iter() {
                            println!("{0: <5} | {1: <32} | {2: <32}", r[0], r[1], r[2]);
                        }
                    }
                    Statement::Insert((k, v)) => {
                        bt.insert(k, v);
                    }
                    Statement::Delete(k) => match bt.delete(&k) {
                        Ok(n_rows) => {
                            println!("{} rows deleted", n_rows);
                        }
                        Err(_) => {
                            println!("row not found");
                        }
                    },
                },
                None => println!("error: statement couldn't be parsed"),
            }
        }
    }

    //Ok(())
}
