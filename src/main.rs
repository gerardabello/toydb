mod vec_mem_table;
mod domain;
//mod sstable;

fn main() {
    let memtable: vec_mem_table::VecMemTable<Box<[u8]>, Box<[u8]>> = vec_mem_table::VecMemTable::new();

    let mut domain = domain::Domain::new(memtable);


    domain.set("a","mandarina");
    domain.set("b","platan");

    assert_eq!(domain.get_string("a").unwrap(), "mandarina");
    assert_eq!(domain.get_string("b").unwrap(), "platan");

}
