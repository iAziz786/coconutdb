use std::io;

pub struct Txn<'a> {
    db: &'a mut super::Db,
}

impl<'a> Txn<'a> {
    pub fn new(db: &'a mut super::Db) -> Self {
        return Self { db };
    }

    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        self.db.set(key, value)
    }
}
