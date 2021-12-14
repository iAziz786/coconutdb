use std::collections::HashMap;

use coconutdb::Coconut;

#[cfg(target_os = "windows")]
const USAGE: &'static str = "
Usage:
    coconutdb.exe FILE get KEY
    coconutdb.exe FILE delete KEY
    coconutdb.exe FILE insert KEY VALUE
    coconutdb.exe FILE update KEY VALUE
";

#[cfg(not(target_os = "windows"))]
const USAGE: &'static str = "
Usage:
    coconutdb FILE get KEY
    coconutdb FILE delete KEY
    coconutdb FILE insert KEY VALUE
    coconutdb FILE update KEY VALUE
";

type ByteString = Vec<u8>;
type ByteStr = [u8];

fn store_index_on_disk(store: &mut Coconut, index_key: &ByteStr) {
    store.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&store.index).unwrap();
    store.index = std::collections::HashMap::new();
    store.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
    const INDEX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = std::env::args().collect();

    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);
    
    let path = std::path::Path::new(&fname);
    let mut store = Coconut::open(path).expect("unable to open the file");
    store.load().expect("unable to load the data");

    match action {
        "get" => {
            let index_as_bytes = store.get(&INDEX_KEY).unwrap().unwrap();

            let index_decoded = bincode::deserialize(&index_as_bytes);
            let index: HashMap<ByteString, u64> = index_decoded.unwrap();
            match index.get(key) {
                None => eprintln!("{:?} not found", key),
                Some(&i) => {
                    let kv = store.get_at(i).unwrap();
                    println!("{:?}", kv.value)
                }
            }
        },
        "delete" => store.delete(key).unwrap(),
        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        },
        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        },
        _ => eprintln!("{}", &USAGE),
    }
}