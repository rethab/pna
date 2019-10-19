extern crate kvs;

use kvs::KvStore;
use kvs::Result;
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = Cmd::from_args();
    let mut kv = KvStore::new();

    match opt {
        Cmd::Set { key, value } => panic!("unimplemented"),
        Cmd::Get { key } => panic!("unimplemented"),
        Cmd::Remove { key } => panic!("unimplemented"),
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
