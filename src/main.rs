use std::io;

use coconutdb::{self, db};

fn txnHandler(txn: &mut db::txn::Txn) -> io::Result<()> {
    txn.set("Hello".to_string(), "World".to_string());
    Ok(())
}

fn main() {
    let mut database = coconutdb::open().unwrap();

    database.update(txnHandler);
}
