extern crate kvs;

use kvs::KvStore;
use kvs::Result;
use std::path::Path;
use std::process;
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = Cmd::from_args();
    let mut kv = KvStore::open(Path::new("/tmp/mydb"))?;

    match opt {
        Cmd::Set { key, value } => kv.set(key, value),
        Cmd::Get { key } => match kv.get(key.clone())? {
            Some(v) => {
                println!("{}", v);
                Ok(())
            }
            None => {
                eprintln!("No value found for key {}", key);
                process::exit(1);
            }
        },
        Cmd::Remove { key } => kv.remove(key),
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "A simple key value store")]
enum Cmd {
    #[structopt(name = "set", about = "Puts a value into the store")]
    Set { key: String, value: String },

    #[structopt(name = "get", about = "Retrieves a value from the store")]
    Get { key: String },

    #[structopt(name = "rm", about = "Removes a value from the store")]
    Remove { key: String },
}
