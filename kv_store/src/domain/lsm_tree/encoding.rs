use std::io::prelude::*;
use std::io::{self, Read, SeekFrom};

fn read_size<Tr: Read + Seek>(reader: &mut Tr, buffer: &mut Vec<u8>) -> io::Result<u16> {
    if buffer.len() < 2 {
        buffer.resize(2, 0);
    }

    reader.read_exact(&mut buffer[..2])?;
    let mut size_bytes = [0u8; 2];
    size_bytes.clone_from_slice(&buffer[..2]);
    let size = u16::from_be_bytes(size_bytes);

    Ok(size)
}

pub fn read_next_datum<Tr: Read + Seek>(
    reader: &mut Tr,
    buffer: &mut Vec<u8>,
) -> io::Result<usize> {
    let size = read_size(reader, buffer)?;

    if buffer.len() < size as usize {
        buffer.resize(size as usize, 0);
    }

    // Read value
    reader.read_exact(&mut buffer[..(size as usize)])?;
    Ok(size as usize)
}

pub fn skip_next_datum<Tr: Read + Seek>(
    reader: &mut Tr,
    buffer: &mut Vec<u8>,
) -> io::Result<usize> {
    let size = read_size(reader, buffer)?;

    reader.seek(SeekFrom::Current(size as i64))?;

    Ok(size as usize)
}

pub fn find_value<Tr: Read + Seek>(reader: &mut Tr, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    // 256 seams a reasonable nubmber to reserve, although values could be as big as
    //     u16::max_value()
    let mut buffer: Vec<u8> = Vec::with_capacity(256);

    loop {
        let key_size = read_next_datum(reader, &mut buffer)?;
        let key_found = &buffer[..key_size];
        let key_found = key == key_found;

        if key_found {
            let value_size = read_next_datum(reader, &mut buffer)?;
            let value = buffer[..(value_size as usize)].to_vec();
            return Ok(Some(value));
        } else {
            skip_next_datum(reader, &mut buffer)?;
        }
    }
}

// Max size is 64kB
fn serialize_size(size: u16) -> [u8; 2] {
    size.to_be_bytes()
}

pub fn serialize_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut ret = Vec::new();
    if key.len() > u16::max_value() as usize {
        panic!("Key bigger than 64kB");
    }

    if value.len() > u16::max_value() as usize {
        panic!("Value bigger than 64kB");
    }

        ret.extend_from_slice(&serialize_size(key.len() as u16));
        ret.append(&mut key.to_vec());
        ret.extend_from_slice(&serialize_size(value.len() as u16));
        ret.append(&mut value.to_vec());
    ret
}

pub fn serialize_values(values: &[(&Vec<u8>, &Vec<u8>)]) -> Vec<u8> {
    let mut ret = Vec::new();
    for p in values {
        if p.0.len() > u16::max_value() as usize {
            panic!("Key bigger than 64kB");
        }

        if p.1.len() > u16::max_value() as usize {
            panic!("Value bigger than 64kB");
        }

        ret.append(&mut serialize_entry(p.0, p.1));
    }

    ret
}
