use std::{
    io::{self, Write},
    path::Path,
};

use crate::Database;

pub fn start_repl() -> io::Result<()> {
    let mut db = Database::open(Path::new("test.db"))?;

    println!("my own sqlite");

    loop {
        print!("mos> ");

        let mut input = String::new();
        io::stdin().read_line(&mut input);
        let input = input.trim();

        match input.split_once(' ') {
            Some(("set", rest)) => {
                if let Some((k, v)) = rest.split_once(' ') {
                    db.set(k, v)?;
                    println!("OK");
                }
            }
            Some(("get", k)) => {
                if let Some(v) = db.get(k) {
                    println!("{}", v);
                } else {
                    println!("NOT FOUND");
                }
            }
            _ if input == "exit" => break,
            _ => println!("unknown command! use: set <key> <value> | get <key> | exit"),
        }
    }

    Ok(())
}
