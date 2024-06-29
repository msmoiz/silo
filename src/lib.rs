use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
};

use anyhow::bail;

pub struct Database {
    index: HashMap<String, u64>,
    log: Log,
}

impl Database {
    /// Starts a Database server.
    pub fn start() -> anyhow::Result<Self> {
        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open("database.log")?;

        let mut log = Log::new(log);

        let mut index = HashMap::new();

        for entry in log.entries() {
            let Ok(entry) = entry else {
                bail!("failed to parse database log");
            };

            match entry.operation {
                Operation::Set(_) => {
                    index.insert(entry.key.clone(), entry.offset);
                }
                Operation::Delete => {
                    index.remove(&entry.key);
                }
            }
        }

        Ok(Self { index, log })
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
        let offset = self.log.append(key, Operation::Set(value.to_owned()))?;
        self.index.insert(key.to_owned(), offset);
        Ok(())
    }

    /// Gets the value for a key.
    pub fn get(&mut self, key: &str) -> anyhow::Result<Option<String>> {
        let Some(offset) = self.index.get(key) else {
            return Ok(None);
        };

        let Operation::Set(value) = self.log.entry_at(*offset)?.operation else {
            bail!("indexed key offset does not point to value");
        };

        Ok(Some(value))
    }

    /// Deletes the value for a key.
    ///
    /// This operation is idempotent and may be repeated multiple times.
    pub fn delete(&mut self, key: &str) -> anyhow::Result<()> {
        self.index.remove(key);
        self.log.append(key, Operation::Delete)?;
        Ok(())
    }
}

/// The operation recorded by a log entry.
enum Operation {
    Set(String),
    Delete,
}

/// An entry in a log file.
struct Entry {
    offset: u64,
    key: String,
    operation: Operation,
}

/// A database log file.
///
/// Represents the official record of database contents.
struct Log {
    inner: File,
}

impl Log {
    /// Creates a new Log from an existing file.
    fn new(inner: File) -> Self {
        Self { inner }
    }

    /// Returns the entry that begins at the provided offset.
    fn entry_at(&mut self, offset: u64) -> anyhow::Result<Entry> {
        self.inner.seek(SeekFrom::Start(offset))?;

        let mut buf = [0; 8];
        self.inner.read_exact(&mut buf)?;
        let key_len = usize::from_be_bytes(buf);

        let mut buf = vec![0; key_len];
        self.inner.read_exact(&mut buf)?;
        let key = String::from_utf8(buf)?;

        let mut buf = [0; 1];
        self.inner.read_exact(&mut buf)?;
        let opcode = buf[0];

        let operation = match opcode {
            0 => {
                let mut buf = [0; 8];
                self.inner.read_exact(&mut buf)?;
                let value_len = usize::from_be_bytes(buf);

                let mut buf = vec![0; value_len];
                self.inner.read_exact(&mut buf)?;
                let value = String::from_utf8(buf)?;

                Operation::Set(value)
            }
            1 => Operation::Delete,
            _ => panic!("unrecognized opcode: {opcode}"),
        };

        Ok(Entry {
            offset,
            key,
            operation,
        })
    }

    /// Appends a new entry to the end of the log.
    ///
    /// Returns the offset of the entry in the log.
    fn append(&mut self, key: &str, operation: Operation) -> anyhow::Result<u64> {
        self.inner.seek(SeekFrom::End(0))?;
        let offset = self.inner.stream_position()?;

        self.inner.write(&key.len().to_be_bytes())?;
        self.inner.write(key.as_bytes())?;
        match operation {
            Operation::Set(value) => {
                self.inner.write(&[0])?;
                self.inner.write(&value.len().to_be_bytes())?;
                self.inner.write(value.as_bytes())?;
            }
            Operation::Delete => {
                self.inner.write(&[1])?;
            }
        }

        Ok(offset)
    }

    /// Returns an iterator over entries in the log.
    fn entries(&mut self) -> Entries {
        Entries::new(self)
    }
}

/// Iterator over entries in a log file.
struct Entries<'a> {
    log: &'a mut Log,
    pos: u64,
}

impl<'a> Entries<'a> {
    /// Creates a new iterator from a log.
    fn new(log: &'a mut Log) -> Self {
        Self { log, pos: 0 }
    }
}

impl<'log> Iterator for Entries<'log> {
    type Item = anyhow::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.log.entry_at(self.pos).ok()?;
        self.pos = self.log.inner.stream_position().ok()?;
        Some(Ok(entry))
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
