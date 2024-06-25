use std::{
    fs,
    io::{BufRead, BufReader, Write},
};

pub struct Database;

impl Database {
    pub fn set(key: &str, value: &str) {
        let mut log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("database.log")
            .unwrap();

        log.write(format!("{key}|{value}\n").as_bytes()).unwrap();
    }

    pub fn get(key: &str) -> Option<String> {
        let log = fs::OpenOptions::new()
            .read(true)
            .open("database.log")
            .unwrap();

        let log = BufReader::new(log);
        let mut lines = log.lines();

        let mut value = None;
        while let Some(Ok(line)) = lines.next() {
            let mut parts = line.split("|");
            if parts.next() == Some(key) {
                value = Some(parts.next().unwrap().to_owned());
            }
        }

        value
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn read_write() {
        Database::set("hello", "world");
        let value = Database::get("hello");
        assert_eq!(value, Some("world".into()))
    }
}
