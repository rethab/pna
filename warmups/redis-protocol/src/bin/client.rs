extern crate redis_protocol;

use redis_protocol::commands::Command;
use redis_protocol::parser::*;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;

use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "redis client")]
struct Opt {
    /// the url (host:port) of the server to connect to
    #[structopt(long)]
    server: String,

    #[structopt(subcommand)]
    cmd: ClientCommand,
}

#[derive(StructOpt)]
enum ClientCommand {
    /// retrieve a key
    Get { key: String },

    /// set a value
    Set { key: String, value: String },

    Ping {
        /// optional message for the server to respond with. when left empty, the server will
        /// respond with PONG
        mb_message: Option<String>,
    },
}

fn main() -> io::Result<()> {
    let opt = Opt::from_args();
    let cmd = match opt.cmd {
        ClientCommand::Get { key } => Command::Get(key),
        ClientCommand::Set { key, value } => Command::Set(key, value),
        ClientCommand::Ping { mb_message } => Command::Ping(mb_message),
    };

    let mut stream = TcpStream::connect(opt.server)?;
    println!("Connected to server");

    let payload = serialize(&cmd);
    stream.write(payload.as_bytes())?;

    match parse_resp_type(&mut stream).unwrap() {
        RespType::SimpleString => {
            if let Ok(resp) = parse_simple_string(&mut stream) {
                println!("{}", resp);
            }
        }
        RespType::BulkString => {
            if let Ok(mb_resp) = parse_bulk_string(&mut stream) {
                match mb_resp {
                    None => println!("(nil)"),
                    Some(resp) => println!("{}", resp),
                }
            }
        }
        unexpected => eprintln!(
            "Only simple and bulk string is implemented, but got: {:?}",
            unexpected
        ),
    }

    Ok(())
}
