use std::{
    fs::File,
    io::{self, BufRead, BufReader, Write},
};
pub mod txn;

pub struct Db {
    pub file: File,
}

impl Db {
    pub fn update<F>(&mut self, cb: F)
    where
        F: Fn(&mut txn::Txn) -> io::Result<()>,
    {
        let mut new_txn = txn::Txn::new(self);
        cb(&mut new_txn);
    }

    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        let data = format!("\r\n{}-{}\r\n", key, value);

        let reader = BufReader::new(&self.file);

        for line in reader.lines() {
            if let Ok(ip) = line {
                println!("{}", ip);
            }
        }

        self.file.write_all(data.as_bytes());

        Ok(())
    }
}
