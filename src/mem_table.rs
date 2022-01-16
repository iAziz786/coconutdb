use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

#[derive(Default)]
pub struct MemTable {
    entires: Vec<MemTableEntry>,
    size: usize,
}

impl MemTable {
    /// Create a new memtable
    pub fn new() -> Self {
        MemTable {
            entires: Vec::new(),
            size: 0,
        }
    }

    /// A binary search to get the index of the key provided
    ///
    /// Will return the [Result::Ok] if the key is present with the value of
    /// existing index, otherwise will return [Result::Err] with the index where
    /// the key _can_ be inserted.
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entires
            .binary_search_by_key(&key, |e| e.key.as_slice())
    }

    /// Set the provided key, value a the memtable
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        let entry = MemTableEntry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("antilock")
                .as_millis(),
            deleted: false,
        };

        match self.get_index(key) {
            Ok(idx) => {
                // size will be calculate if entry already exists
                if let Some(v) = self.entires[idx].value.as_ref() {
                    if value.len() < v.len() {
                        self.size -= v.len() - value.len();
                    } else {
                        self.size += value.len() - v.len();
                    }
                } else {
                    self.size += value.len();
                }
                self.entires[idx] = entry;
            }

            Err(idx) => {
                self.size += entry.key.len()
                    + entry.value.as_ref().unwrap().len()
                    + mem::size_of_val(&entry.timestamp)
                    + mem::size_of_val(&entry.deleted);
                self.entires.insert(idx, entry)
            }
        }
    }

    /// Delete the provided key
    pub fn delete(&mut self, key: &[u8]) {
        if let Ok(idx) = self.get_index(key) {
            // size will be calculate if entry already exists
            let element = &mut self.entires[idx];
            if !element.deleted {
                element.deleted = true;
                element.timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("antilock")
                    .as_millis();
            }
        }
    }

    /// Get will return the mem table entry if present.
    ///
    /// Otherwise it returns [None]
    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(idx) = self.get_index(key) {
            return Some(&self.entires[idx]);
        }
        None
    }

    /// Len returns number of entries present in the memtable
    pub fn len(&self) -> usize {
        self.entires.len()
    }

    /// Check if there are no entires in the memtable
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Entries will return the reference to the entries in the memtable
    pub fn entries(&self) -> &[MemTableEntry] {
        &self.entires
    }

    /// Gets the total size of the records in the MemTable
    pub fn size(&self) -> usize {
        self.size
    }
}
