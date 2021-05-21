use std::{fs::OpenOptions, io};
pub mod db;

pub fn open() -> io::Result<db::Db> {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .write(true)
        .open("my.log")?;

    Ok(db::Db { file: file })
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
