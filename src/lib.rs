use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
};

pub struct Database {
    index: HashMap<String, u64>,
    log: File,
}

impl Database {
    /// Creates a new Database instance.
    pub fn new() -> anyhow::Result<Self> {
        fs::remove_file("database.log").ok();

        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open("database.log")?;

        Ok(Self {
            index: HashMap::new(),
            log,
        })
    }

    pub fn set(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        let offset = self.log.stream_position()?;
        self.index.insert(key.to_owned(), offset);
        self.log.write(&key.len().to_be_bytes())?;
        self.log.write(key.as_bytes())?;
        self.log.write(&value.len().to_be_bytes())?;
        self.log.write(value.as_bytes())?;
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> anyhow::Result<Option<String>> {
        let Some(offset) = self.index.get(key) else {
            return Ok(None);
        };

        let restore = self.log.stream_position()?;
        self.log.seek(SeekFrom::Start(*offset))?;

        // skip the key_len and key
        let mut buf = [0; 8];
        self.log.read_exact(&mut buf)?;
        let key_len = usize::from_be_bytes(buf);
        self.log.seek(SeekFrom::Current(key_len.try_into()?))?;

        // read the value
        let mut buf = [0; 8];
        self.log.read_exact(&mut buf)?;
        let value_len = usize::from_be_bytes(buf);
        let mut buf = vec![0; value_len];
        self.log.read_exact(&mut buf)?;
        let value = String::from_utf8(buf)?;

        self.log.seek(SeekFrom::Start(restore))?;

        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn read_write() {
        let mut database = Database::new().unwrap();

        database.set("hello", "sun").unwrap();
        database.set("goodbye", "moon").unwrap();
        database.set("farewell", "sky").unwrap();

        let hello = database.get("hello").unwrap();
        let goodbye = database.get("goodbye").unwrap();
        let farewell = database.get("farewell").unwrap();

        assert_eq!(hello, Some("sun".into()));
        assert_eq!(goodbye, Some("moon".into()));
        assert_eq!(farewell, Some("sky".into()));
    }
}
