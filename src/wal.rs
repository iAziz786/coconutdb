use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter};
use std::io::{prelude::*, BufReader};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use coconutdb::mem_table::MemTable;

pub struct WAL {
    path: PathBuf,
    file: BufWriter<File>,
}

pub struct WalEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

/// WALIterator will iterate over the items in WAL
pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    /// Create a new WALIterator from a path
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIterator { reader })
    }
}

impl Iterator for WALIterator {
    type Item = WalEntry;

    /// Gets the next entry in the WAL
    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buffer = [0; 8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }

        // key size is dynamic, so read what's the key size
        let key_len = usize::from_le_bytes(len_buffer);

        let mut bool_buffer = [0; 1];
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;

        let mut key = vec![0; key_len];
        let mut value = None;

        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf);
        }

        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WalEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

impl WAL {
    /// Create new WAL at the given path, if
    pub fn new(dir: &Path) -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL { path, file })
    }

    /// Create a WAL from an already existing file
    pub fn from_path(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        let file = BufWriter::new(file);

        Ok(WAL {
            path: path.to_owned(),
            file,
        })
    }

    /// Load WAL(s) from a direction and creates a WAL and MemTable
    ///
    /// Multiple WAL files are sorted based on their name which is the timestamp
    /// at which the file was created and creates the WAL and MemTable based on
    /// their replays
    pub fn load_from_dir(dir: &Path) -> io::Result<(WAL, MemTable)> {
        let files = fs::read_dir(dir).unwrap();

        let mut wal_paths = Vec::new();
        for file in files {
            let path = file.unwrap().path();
            if path.extension().unwrap() == ".wal" {
                wal_paths.push(path);
            }
        }

        // Sorting is important because we will replay the WAL and MemTable
        // based on the order of the files
        wal_paths.sort();

        let mut new_wal = WAL::new(dir)?;
        let mut new_mem_table = MemTable::new();
        for wal_path in wal_paths.iter() {
            if let Ok(wal) = WAL::from_path(wal_path) {
                for entry in wal.into_iter() {
                    if entry.deleted {
                        new_mem_table.delete(entry.key.as_slice());
                        new_wal.delete(entry.key.as_slice());
                    } else {
                        new_mem_table.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                        );
                        new_wal.set(
                            entry.key.as_slice(),
                            entry.value.as_ref().unwrap().as_slice(),
                        );
                    }
                }
            }
        }

        new_wal.flush().unwrap();
        wal_paths
            .into_iter()
            .for_each(|f| fs::remove_file(f).unwrap());

        Ok((new_wal, new_mem_table))
    }

    /// Set will append the key-value pair to the WAL
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(
            &(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis())
            .to_le_bytes(),
        )?;

        Ok(())
    }

    /// Delete will mark the provided key as delete to the WAL
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Flushes the WAL to persistent store.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl IntoIterator for WAL {
    type IntoIter = WALIterator;
    type Item = WalEntry;

    /// Converts a WAL into a `WALIterator` to iterate over the entries.
    fn into_iter(self) -> WALIterator {
        WALIterator::new(self.path).unwrap()
    }
}
