mod arrayMemTable;
mod domain;
//mod sstable;

fn main() {
    let memtable = arrayMemTable::ArrayMemTable::new();

    let domain = domain::Domain::new(memtable);

let boxed_str: Box<[u8]> = Box::from(Box::from("hello"));

    domain.set()
}
