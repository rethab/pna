extern crate redis_protocol;

use redis_protocol::commands::Command::*;
use redis_protocol::parser;
use std::io;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    println!("Listening on port 6379");
    for stream in listener.incoming() {
        handle_client(&mut stream?)?;
    }
    Ok(())
}

fn handle_client(stream: &mut TcpStream) -> io::Result<()> {
    match parser::deserialize(stream) {
        Ok(cmd) => match cmd {
            Get(key) => {
                println!("Get({})", key);
                stream.write(&parser::encode_null_bulk_string())?;
            }
            Set(key, value) => {
                println!("Set({},{})", key, value);
                stream.write(&parser::encode_null_bulk_string())?;
            }
            Ping(mb_message) => {
                println!(
                    "Ping({})",
                    mb_message.clone().unwrap_or_else(|| "".to_owned())
                );
                let resp = mb_message.unwrap_or_else(|| "PONG".to_owned());
                stream.write(&parser::encode_simple_string(&resp))?;
            }
        },
        Err(msg) => eprintln!("Failed to parse command from input: {}", msg),
    }
    Ok(())
}
