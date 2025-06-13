use std::{io, path::Path};

use crate::{
    Database,
    sql::{SqlCommand, parse_sql},
};

pub fn start_repl() -> io::Result<()> {
    let mut db = Database::load(Path::new("mos.db"))?;

    println!("my own sqlite");

    loop {
        print!("mos> ");

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input == "exit" {
            db.save(Path::new("mos.db"))?;
            break;
        }

        match parse_sql(input) {
            Some(SqlCommand::CreateTable { name, columns }) => {
                if let Err(e) = db.create_table(&name, columns) {
                    println!("Error: {}", e);
                } else {
                    println!("Table '{}' created", name);
                }
            }
            Some(SqlCommand::Insert { table, values }) => {
                if let Err(e) = db.insert_into(&table, values) {
                    println!("Error: {}", e);
                } else {
                    println!("OK");
                }
            }
            Some(SqlCommand::Select {
                table,
                columns,
                where_clause,
            }) => {
                let where_args = where_clause.map(|(col, val)| (col.to_string(), val.to_string()));
                match db.select_from(&table, &columns, where_args) {
                    Ok(rows) => {
                        if rows.is_empty() {
                            println!("(no rows)");
                        } else {
                            for row in rows {
                                println!("{}", row.join(" | "));
                            }
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            None => println!("Parse error"),
        }
    }

    Ok(())
}
