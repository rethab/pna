#[macro_use]
extern crate serde;
extern crate bson;
extern crate ron;
extern crate serde_json;

use json_sample::MError::*;
use std::error::Error;
use std::path::Path;
use std::str::from_utf8;

mod bson_sample;
mod json_sample;
mod ron_sample;

fn main() {
    {
        // part 1: json
        // error handling: https://blog.burntsushi.net/rust-error-handling/#composing-custom-error-types
        let a = json_sample::Move::new();
        let file_name = Path::new("/tmp/move.json");
        match json_sample::write_to_file(&a, &file_name) {
            Ok(nbytes) => println!("Written {} bytes to file: {:?}", nbytes, a),
            Err(err) => match err {
                Serialization(u) => {
                    eprintln!("JSON: Failed to serialize: {}", u.description().to_owned())
                }
                IO(u) => eprintln!(
                    "JSON: Failed to write to file: {}",
                    u.description().to_owned()
                ),
            },
        }

        let b = json_sample::read_from_file(&file_name).unwrap();
        println!("JSON: Read from file: {:?}", b);
    }

    {
        // part 2: ron
        let a = ron_sample::Move::new();
        let mut buffer = Vec::new();
        ron_sample::write_to_buffer(&a, &mut buffer);
        println!("RON: Written");
        let b = ron_sample::read_from_buffer(&buffer).unwrap();
        println!("RON: Read from buffer: {:?}", b);

        let str_move = from_utf8(&buffer).unwrap();
        println!("RON: Move as string: {}", str_move);
    }

    {
        // part 3: bson
        let mut ms = Vec::with_capacity(1000);
        for _ in 0..999 {
            ms.push(bson_sample::Move::new());
        }
        let file_name = Path::new("/tmp/moves.bson");
        match bson_sample::write_to_file(&ms, &file_name) {
            Ok(_) => {
                println!("BSON: Written to file");
                let ms = bson_sample::read_from_file(&file_name);
                println!(
                    "BSON: Read {} moves from file. Eg: move[33]={:?}",
                    ms.len(),
                    ms[33]
                );
            }
            Err(error) => eprintln!("BSON: Failed to write: {}", error),
        }
    }

    {
        // part 3 b: bson with vector
        let mut ms = Vec::with_capacity(1000);
        for _ in 0..999 {
            ms.push(bson_sample::Move::new());
        }
        let mut buf = bson_sample::VecWriter(Vec::with_capacity(1000));
        match bson_sample::write_to_buf(&ms, &mut buf) {
            Ok(_) => {
                println!("BSON: Written to buffer. Size={}", buf.0.len());
                let ms = bson_sample::read_from_buf(&mut buf);
                println!(
                    "BSON: Read {} moves from buffer. Eg: move[33]={:?}",
                    ms.len(),
                    ms[33]
                );
            }
            Err(error) => eprintln!("BSON: Failed to write: {}", error),
        }
    }
}
