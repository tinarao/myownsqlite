mod repl;
mod sql;

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{self, ErrorKind, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::repl::start_repl;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Text,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    tables: HashMap<String, Table>,
}

const DEFAULT_DB_PATH: &str = "mos.db";

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        if data.is_empty() {
            Ok(Database {
                tables: HashMap::new(),
            })
        } else {
            bincode::deserialize(&data).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
        }
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        let data = bincode::serialize(self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;

        file.write_all(&data)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_functions() {
        let db_path = Path::new(&DEFAULT_DB_PATH);
        let mut db = match Database::load(db_path) {
            Ok(d) => d,
            Err(e) => panic!("{}", e),
        };

        let cols: Vec<Column> = vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
            },
            Column {
                name: "email".to_string(),
                data_type: DataType::Text,
            },
        ];

        match db.create_table("jobs", cols) {
            Ok(()) => {}
            Err(e) => panic!("{}", e),
        };

        let test_email = "example@main.rs".to_string();
        let values = vec![1.to_string(), test_email.clone()];
        match db.insert_into("jobs", values) {
            Ok(()) => {}
            Err(e) => panic!("{}", e),
        };

        let values = db.select_from(
            "jobs",
            &["email".to_string()],
            Some(("id".to_string(), "1".to_string())),
        );

        let email = &values.unwrap()[0][0];
        assert_eq!(email, &test_email);
    }
}
