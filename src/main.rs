mod repl;
mod sql;

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Read,
    path::Path,
};

use crate::repl::start_repl;

#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Text,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
}

pub struct Database {
    tables: HashMap<String, Table>,
    file: File,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, std::io::Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        // let data = contents
        //     .lines()
        //     .filter_map(|line| line.split_once(":"))
        //     .map(|(k, v)| (k.to_string(), v.to_string()))
        //     .collect();

        Ok(Database {
            tables: HashMap::new(),
            file,
        })
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<(), String> {
        if self.tables.contains_key(name) {
            return Err(format!("Table {} already exists", name));
        }

        self.tables.insert(
            name.to_string(),
            Table {
                name: name.to_string(),
                columns,
                rows: Vec::new(),
            },
        );

        Ok(())
    }

    pub fn insert_into(&mut self, table_name: &str, values: Vec<String>) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or(format!("Table {} does not exist", table_name))?;

        if values.len() != table.columns.len() {
            return Err(format!(
                "Expected {} values. Got {}",
                table.columns.len(),
                values.len()
            ));
        }

        for (_, (val, column)) in values.iter().zip(table.columns.iter()).enumerate() {
            match column.data_type {
                DataType::Integer => {
                    if val.parse::<i64>().is_err() {
                        return Err(format!(
                            "Value '{}' is not INTEGER for column {}",
                            val, column.name
                        ));
                    }
                }
                DataType::Text => {}
            }
        }

        table.rows.push(values);
        Ok(())
    }

    pub fn select_from(
        &self,
        table_name: &str,
        columns: &[String],
        where_clause: Option<(String, String)>,
    ) -> Result<Vec<Vec<String>>, String> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        let selected_columns: Vec<usize> = columns
            .iter()
            .map(|col| {
                table
                    .columns
                    .iter()
                    .position(|c| &c.name == col)
                    .ok_or(format!("Column '{}' not found", col))
            })
            .collect::<Result<_, _>>()?;

        let filtered_rows: Vec<&Vec<String>> = if let Some((col, value)) = where_clause {
            let col_index = table
                .columns
                .iter()
                .position(|c| c.name == col)
                .ok_or(format!("WHERE column '{}' not found", col))?;
            table
                .rows
                .iter()
                .filter(|row| row[col_index] == value)
                .collect()
        } else {
            table.rows.iter().collect()
        };

        let mut result = Vec::new();
        for row in filtered_rows {
            let mut selected_row: Vec<String> = Vec::with_capacity(selected_columns.len());
            for &col_idx in selected_columns.iter() {
                selected_row.push(row[col_idx].clone());
            }
            result.push(selected_row);
        }
        Ok(result)

        // fn sync(&mut self) -> Result<(), std::io::Error> {
        //     let mut contents = String::new();
        //     for (k, v) in &self.data {
        //         contents.push_str(&format!("{}:{}\n", k, v));
        //     }
        //
        //     self.file.set_len(0)?;
        //     self.file.write_all(contents.as_bytes())
        // }
    }
}

fn main() {
    start_repl();
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn write() {
//         // let key = "aboba";
//         // let val = "aboba_val";

//         // let path = Path::new("db.db");
//         // let mut db = match Database::open(path) {
//         //     Ok(d) => d,
//         //     Err(e) => panic!("{}", e),
//         // };

//         // if let Err(e) = db.set(key, val) {
//         //     panic!("{}", e)
//         // }

//         // match db.get(key) {
//         //     Some(v) => {
//         //         assert_eq!(val, v);
//         //     }
//         //     None => panic!("returned None"),
//         // };
//     }
// }
