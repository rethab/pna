use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::vec::Vec;

use bson;

#[derive(Serialize, Deserialize, Debug)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Move {
    steps: i32,
    direction: Direction,
}

impl Move {
    pub fn new() -> Move {
        Move {
            steps: 12,
            direction: Direction::Left,
        }
    }
}

pub fn write_to_file(ms: &[Move], file_name: &Path) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_name)
        .map_err(|err| err.description().to_owned())?;

    for m in ms {
        let bson = bson::to_bson(m).map_err(|err| err.description().to_owned())?;
        let doc = bson
            .as_document()
            .ok_or("failed to create document".to_owned())?;
        let mut buf = Vec::new();
        bson::encode_document(&mut buf, doc).map_err(|err| err.description().to_owned())?;
        file.write_all(&buf)
            .map_err(|err| err.description().to_owned())?;
    }
    Ok(())
}

pub fn read_from_file(file_name: &Path) -> Vec<Move> {
    let mut f = File::open(file_name).unwrap();
    let mut buf = Vec::new();
    while let Ok(val) = bson::decode_document(&mut f) {
        buf.push(bson::from_bson(bson::Bson::Document(val)).unwrap());
    }
    buf
}

pub struct VecWriter(pub Vec<u8>);

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        <Vec<u8> as Write>::write(&mut self.0, buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        <Vec<u8> as Write>::flush(&mut self.0)
    }
}

impl Read for VecWriter {
    // copied from the read impl for Vec<u8>
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let amt = if buf.len() < self.0.len() {
            buf.len()
        } else {
            self.0.len()
        };
        let (a, b) = self.0.split_at(amt);

        // First check if the amount of bytes we want to read is small:
        // `copy_from_slice` will generally expand to a call to `memcpy`, and
        // for a single byte the overhead is significant.
        if amt == 1 {
            buf[0] = a[0];
        } else {
            buf[..amt].copy_from_slice(a);
        }

        self.0 = b.to_vec();
        Ok(amt)
    }
}

pub fn write_to_buf(ms: &[Move], mut buf: &mut VecWriter) -> Result<(), String> {
    for m in ms {
        let bson = bson::to_bson(m).map_err(|err| err.description().to_owned())?;
        let doc = bson
            .as_document()
            .ok_or("failed to create document".to_owned())?;
        bson::encode_document(&mut buf, doc).map_err(|err| err.description().to_owned())?;
    }
    Ok(())
}

pub fn read_from_buf(mut buf: &mut VecWriter) -> Vec<Move> {
    let mut res = Vec::new();
    while let Ok(val) = bson::decode_document(&mut buf) {
        res.push(bson::from_bson(bson::Bson::Document(val)).unwrap());
    }
    res
}
