use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

pub struct Database {
    data: HashMap<String, String>,
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
        file.read_to_string(&mut contents);

        let data = contents
            .lines()
            .filter_map(|line| line.split_once(":"))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        Ok(Database { data, file })
    }

    pub fn set(&mut self, key: &str, val: &str) -> Result<(), std::io::Error> {
        self.data.insert(key.to_string(), val.to_string());
        self.sync();
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    fn sync(&mut self) -> Result<(), std::io::Error> {
        let mut contents = String::new();
        for (k, v) in &self.data {
            contents.push_str(&format!("{}:{}\n", k, v));
        }

        self.file.set_len(0)?;
        self.file.write_all(contents.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write() {
        let key = "aboba";
        let val = "aboba_val";

        let path = Path::new("db.db");
        let mut db = match Database::open(path) {
            Ok(d) => d,
            Err(e) => panic!("{}", e),
        };

        if let Err(e) = db.set(key, val) {
            panic!("{}", e)
        }

        match db.get(key) {
            Some(v) => {
                assert_eq!(val, v);
            }
            None => panic!("returned None"),
        };
    }
}
