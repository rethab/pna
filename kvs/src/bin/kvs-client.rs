extern crate kvs;

use std::process;
use structopt::StructOpt;

mod protocol {
    tonic::include_proto!("kvs");
}

use protocol::{client::KvsClient, GetRequest, RemoveRequest, SetRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd = Cmd::from_args();

    let client = |mb_addr: Option<String>| {
        let addr = mb_addr
            .map(|a| {
                if !a.starts_with("http") {
                    format!("http://{}", a)
                } else {
                    a
                }
            })
            .unwrap_or_else(|| "http://127.0.0.1:4000".to_owned());
        KvsClient::connect(addr)
    };

    match cmd {
        Cmd::Set { key, value, addr } => {
            let req = tonic::Request::new(SetRequest { key, value });
            client(addr).await?.set(req).await?;
        }
        Cmd::Get { key, addr } => {
            let req = tonic::Request::new(GetRequest { key });
            let resp = client(addr).await?.get(req).await?;
            match resp.into_inner().value {
                Some(v) => println!("{}", v.value),
                None => println!("Key not found"),
            }
        }
        Cmd::Remove { key, addr } => {
            let req = tonic::Request::new(RemoveRequest { key });
            let resp = client(addr).await?.remove(req).await?;
            if !resp.into_inner().removed {
                eprintln!("Key not found");
                process::exit(1);
            }
        }
    };

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(about = "A simple key value store")]
enum Cmd {
    #[structopt(name = "set", about = "Puts a value into the store")]
    Set {
        key: String,
        value: String,
        #[structopt(long)]
        addr: Option<String>,
    },

    #[structopt(name = "get", about = "Retrieves a value from the store")]
    Get {
        key: String,
        #[structopt(long)]
        addr: Option<String>,
    },

    #[structopt(name = "rm", about = "Removes a value from the store")]
    Remove {
        key: String,
        #[structopt(long)]
        addr: Option<String>,
    },
}
