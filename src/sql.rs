use crate::{Column, DataType};

pub enum SqlCommand {
    Insert {
        table: String,
        values: Vec<String>,
    },
    Select {
        table: String,
        columns: Vec<String>,
        where_clause: Option<(String, String)>, // (Column, Value)
    },
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
}

pub fn parse_sql(input: &str) -> Option<SqlCommand> {
    let input = input.to_lowercase();
    let mut words = input.split_whitespace();

    match words.next()? {
        "create" => {
            if words.next()? != "table" {
                return None;
            }

            let name = words.next()?.to_string();
            let mut columns = Vec::new();

            if input.contains('(') {
                let cols_part = input.split('(').nth(1)?.split(')').next()?;
                for col_def in cols_part.split(',') {
                    let mut parts = col_def.trim().split_whitespace();
                    let name = parts.next()?.to_string();
                    let data_type = match parts.next()? {
                        "integer" => DataType::Integer,
                        "text" => DataType::Text,
                        _ => return None,
                    };

                    columns.push(Column { name, data_type })
                }
            }

            Some(SqlCommand::CreateTable { name, columns })
        }

        "insert" => {
            if words.next()? != "into" {
                return None;
            }

            let table = words.next()?.to_string();
            if words.next()? != "values" {
                return None;
            }

            let values_part = input.split('(').nth(1)?.split(')').next()?;
            let values = values_part
                .split(',')
                .map(|s| s.trim().trim_matches('\'').to_string())
                .collect();

            Some(SqlCommand::Insert { table, values })
        }
        "select" => {
            let columns: Vec<String> = words
                .next()?
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();

            if words.next()? != "from" {
                return None;
            }
            let table = words.next()?.to_string();

            let mut where_clause = None;
            if words.next() == Some("where") {
                let column = words.next()?.to_string();
                if words.next()? != "=" {
                    return None;
                }

                let value = words.next()?.to_string();
                where_clause = Some((column, value));
            }

            Some(SqlCommand::Select {
                table,
                columns,
                where_clause,
            })
        }
        _ => None,
    }
}
