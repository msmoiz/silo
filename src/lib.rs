use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
};

pub struct Database {
    index: HashMap<String, u64>,
    log: File,
}

impl Database {
    /// Starts a Database server.
    pub fn start() -> anyhow::Result<Self> {
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

    /// Starts an interactive session with a Database.
    pub fn repl() -> anyhow::Result<()> {
        let mut database = Self::start()?;
        loop {
            let mut line = String::new();
            io::stdin().read_line(&mut line)?;
            line.pop(); // strip newline

            if line == "exit" {
                println!("-> exiting");
                break;
            }

            match line.split_once(' ') {
                None => println!("-> err: unsupported command"),
                Some(("get", key)) => match database.get(key)? {
                    Some(value) => println!("-> {value}"),
                    None => println!("-> null"),
                },
                Some(("set", key_value)) => match key_value.split_once(' ') {
                    Some((key, value)) => {
                        database.set(key, value)?;
                        println!("-> set {key}")
                    }
                    None => println!("-> err: missing value"),
                },
                Some(("del", key)) => {
                    database.delete(key)?;
                    println!("-> deleted {key}");
                }
                _ => println!("-> err: unsupported command"),
            }
        }

        Ok(())
    }

    /// Sets the value for a key.
    pub fn set(&mut self, key: &str, value: &str) -> anyhow::Result<()> {
        let offset = self.log.stream_position()?;
        self.index.insert(key.to_owned(), offset);
        self.log.write(&key.len().to_be_bytes())?;
        self.log.write(key.as_bytes())?;
        self.log.write(&[0])?; // value
        self.log.write(&value.len().to_be_bytes())?;
        self.log.write(value.as_bytes())?;
        Ok(())
    }

    /// Gets the value for a key.
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

        // skip the type, expected to be value
        self.log.seek(SeekFrom::Current(1))?;

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

    /// Deletes the value for a key.
    ///
    /// This operation is idempotent and may be repeated multiple times.
    pub fn delete(&mut self, key: &str) -> anyhow::Result<()> {
        self.index.remove(key);
        self.log.write(&key.len().to_be_bytes())?;
        self.log.write(key.as_bytes())?;
        self.log.write(&[1])?; // tombstone
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn read_write() {
        let mut database = Database::start().unwrap();

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

    #[test]
    fn delete() {
        let mut database = Database::start().unwrap();

        database.set("hello", "world").unwrap();
        database.delete("hello").unwrap();

        let hello = database.get("hello").unwrap();

        assert_eq!(hello, None);
    }
}
