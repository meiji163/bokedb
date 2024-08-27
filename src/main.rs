use std::io::{self, BufRead, Write};
use std::process::exit;

mod storage;
mod types;
use storage::btree;
use types::values::*;

#[derive(Debug)]
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

fn main() -> io::Result<()> {
    let mut input_buf = String::with_capacity(4096);
    let mut stdin = io::stdin().lock();

    //////
    let myint = Value::Int(51);
    println!("{:?}", myint);

    //impl btree::Key for i32 {}
    let mut mytree: btree::BTree<i32, i32> = btree::BTree::new(3);
    mytree.insert(5, 5);
    //////

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
            //
        }
    }

    //Ok(())
}
