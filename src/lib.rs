use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use crc::CRC_16_IBM_SDLC;
use ulid::Ulid;

const SILO_DIR: &'static str = "silo";

enum IndexEntry {
    Offset(u64),
    Tombstone,
}

pub struct Index(HashMap<String, IndexEntry>);

/// A thread-safe object with locking mechanics.
type Shared<T> = Arc<Mutex<T>>;

pub struct Database {
    logs: Shared<Vec<Log>>,
    indices: Shared<Vec<Index>>,
}

impl Database {
    /// Starts a Database server.
    pub fn start() -> anyhow::Result<Self> {
        fs::create_dir(SILO_DIR).ok();

        let mut logs = vec![]; // implicitly sorted chronologically
        let mut indices = vec![];

        for dir_entry in fs::read_dir(SILO_DIR)? {
            let path = dir_entry?.path();

            let log = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)?;

            let mut log = Log::from_file(log, path);

            let mut index = HashMap::new();
            for entry in log.entries() {
                let Ok(entry) = entry else {
                    bail!("failed to parse database log");
                };

                match entry.operation {
                    Operation::Set(_) => {
                        index.insert(entry.key.clone(), IndexEntry::Offset(entry.offset));
                    }
                    Operation::Delete => {
                        index.insert(entry.key.clone(), IndexEntry::Tombstone);
                    }
                }
            }

            logs.push(log);
            indices.push(Index(index));
        }

        if logs.is_empty() {
            logs.push(Log::new()?);
            indices.push(Index(HashMap::new()));
        }

        let logs = Arc::new(Mutex::new(logs));
        let indices = Arc::new(Mutex::new(indices));

        let database = Self {
            logs: logs.clone(),
            indices: indices.clone(),
        };

        thread::spawn(|| {
            if let Err(e) = Self::compact_logs(logs, indices) {
                eprintln!("log compaction failed: {e}");
            }
        });

        Ok(database)
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
        let mut logs = self.logs.lock().unwrap();
        let mut indices = self.indices.lock().unwrap();

        let offset = logs
            .last_mut()
            .unwrap()
            .append(key, Operation::Set(value.to_owned()))?;

        indices
            .last_mut()
            .unwrap()
            .0
            .insert(key.to_owned(), IndexEntry::Offset(offset));

        const MAX_FILE_SIZE_BYTES: u64 = 4 * (1024u64.pow(3)); // 4MB
        if logs.last_mut().unwrap().size()? >= MAX_FILE_SIZE_BYTES {
            logs.push(Log::new()?);
        }

        Ok(())
    }

    /// Gets the value for a key.
    pub fn get(&mut self, key: &str) -> anyhow::Result<Option<String>> {
        let mut logs = self.logs.lock().unwrap();
        let indices = self.indices.lock().unwrap();

        for (i, Index(index)) in indices.iter().rev().enumerate() {
            let Some(index_entry) = index.get(key) else {
                continue;
            };

            let IndexEntry::Offset(offset) = index_entry else {
                return Ok(None);
            };

            let log_idx = logs.len() - i - 1;
            let Operation::Set(value) = logs[log_idx].entry_at(*offset)?.operation else {
                bail!("indexed key offset does not point to value");
            };

            return Ok(Some(value));
        }

        Ok(None)
    }

    /// Deletes the value for a key.
    ///
    /// This operation is idempotent and may be repeated multiple times.
    pub fn delete(&mut self, key: &str) -> anyhow::Result<()> {
        let mut logs = self.logs.lock().unwrap();
        let mut indices = self.indices.lock().unwrap();

        indices.last_mut().unwrap().0.remove(key);
        logs.last_mut().unwrap().append(key, Operation::Delete)?;
        Ok(())
    }

    /// Compacts log files in the background.
    fn compact_logs(logs: Shared<Vec<Log>>, indices: Shared<Vec<Index>>) -> anyhow::Result<()> {
        loop {
            thread::sleep(Duration::from_secs(300));

            for dir_entry in fs::read_dir(SILO_DIR)? {
                let path = dir_entry?.path();

                let mut log = {
                    let file = File::open(&path)?;
                    Log::from_file(file, path.clone())
                };

                let mut entries = HashMap::<String, Operation>::new();
                let mut should_compact = false;

                for entry in log.entries() {
                    let Entry { key, operation, .. } = entry?;
                    let previous = entries.insert(key, operation);
                    if previous.is_some() {
                        should_compact = true;
                    }
                }

                if should_compact {
                    let source_name = path
                        .file_stem()
                        .expect("should only be reading .log files")
                        .to_str()
                        .unwrap();

                    let source_id = Ulid::from_string(source_name)?;
                    let compacted_id = source_id.increment().unwrap();
                    let mut compacted_log = Log::new_with_id(compacted_id)?;

                    for (key, operation) in entries.into_iter() {
                        compacted_log.append(&key, operation)?;
                    }

                    // grab locks
                    let mut logs = logs.lock().unwrap();
                    let mut indices = indices.lock().unwrap();
                    let pos = logs.iter().position(|log| log.path == path).unwrap();
                    let mut index = Index(HashMap::new());
                    for entry in compacted_log.entries() {
                        let entry = entry?;
                        index.0.insert(
                            entry.key,
                            match entry.operation {
                                Operation::Set(_) => IndexEntry::Offset(entry.offset),
                                Operation::Delete => IndexEntry::Tombstone,
                            },
                        );
                    }
                    logs[pos] = compacted_log;
                    indices[pos] = index;
                }
            }
        }
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
    path: PathBuf,
}

impl Log {
    /// Creates a new Log.
    ///
    /// This creates a new log file on disk as well.
    fn new() -> anyhow::Result<Self> {
        let id = Ulid::new();
        Self::new_with_id(id)
    }

    /// Creates a new Log with a name based on the provided id.
    fn new_with_id(id: Ulid) -> anyhow::Result<Self> {
        let path = PathBuf::from(SILO_DIR).join(format!("{id}.log"));

        let inner = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        Ok(Self { inner, path })
    }

    /// Creates a Log from an existing file.
    fn from_file(inner: File, path: PathBuf) -> Self {
        Self { inner, path }
    }

    /// Returns the entry that begins at the provided offset.
    fn entry_at(&mut self, offset: u64) -> anyhow::Result<Entry> {
        self.inner.seek(SeekFrom::Start(offset))?;

        let mut buf = [0; 2];
        self.inner.read_exact(&mut buf)?;
        let expected_checksum = u16::from_be_bytes(buf);

        let mut buf = [0; 8];
        self.inner.read_exact(&mut buf)?;
        let timestamp = u64::from_be_bytes(buf);

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

        let actual_checksum = {
            let crc = crc::Crc::<u16>::new(&CRC_16_IBM_SDLC);
            let mut digest = crc.digest();
            digest.update(&timestamp.to_be_bytes());
            digest.update(&key.len().to_be_bytes());
            digest.update(&key.as_bytes());
            match &operation {
                Operation::Set(value) => {
                    digest.update(&[0]);
                    digest.update(&value.len().to_be_bytes());
                    digest.update(value.as_bytes());
                }
                Operation::Delete => {
                    digest.update(&[1]);
                }
            }
            digest.finalize()
        };

        if actual_checksum != expected_checksum {
            bail!("checksums do not match for entry at position {offset}")
        }

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

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let checksum = {
            let crc = crc::Crc::<u16>::new(&CRC_16_IBM_SDLC);
            let mut digest = crc.digest();
            digest.update(&timestamp.to_be_bytes());
            digest.update(&key.len().to_be_bytes());
            digest.update(&key.as_bytes());
            match &operation {
                Operation::Set(value) => {
                    digest.update(&[0]);
                    digest.update(&value.len().to_be_bytes());
                    digest.update(value.as_bytes());
                }
                Operation::Delete => {
                    digest.update(&[1]);
                }
            }
            digest.finalize()
        };

        self.inner.write(&checksum.to_be_bytes())?;
        self.inner.write(&timestamp.to_be_bytes())?;
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

    /// Returns the current size of the log in bytes.
    fn size(&self) -> anyhow::Result<u64> {
        let size = self.inner.metadata()?.size();
        Ok(size)
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
