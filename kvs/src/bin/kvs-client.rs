extern crate kvs;

use kvs::KvError;
use kvs::KvStore;
use kvs::Result;
use std::path::Path;
use std::process;
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let mut kv = KvStore::open(Path::new("."))?;

    match opt.cmd {
        Cmd::Set { key, value } => kv.set(key, value),
        Cmd::Get { key } => match kv.get(key.clone())? {
            Some(v) => {
                println!("{}", v);
                Ok(())
            }
            None => {
                println!("Key not found");
                Ok(())
            }
        },
        Cmd::Remove { key } => match kv.remove(key) {
            Err(KvError::KeyNotFound) => {
                println!("Key not found");
                process::exit(1);
            }
            x => x,
        },
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "A simple key value store")]
struct Opt {
    // the server's address (IP:PORT). Defaults to 127.0.0.1:4000 if not specified.
    #[structopt(long)]
    addr: Option<String>,
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, StructOpt)]
enum Cmd {
    #[structopt(name = "set", about = "Puts a value into the store")]
    Set { key: String, value: String },

    #[structopt(name = "get", about = "Retrieves a value from the store")]
    Get { key: String },

    #[structopt(name = "rm", about = "Removes a value from the store")]
    Remove { key: String },
}
