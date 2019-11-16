extern crate kvs;

use structopt::StructOpt;

mod protocol {
    tonic::include_proto!("kvs");
}

use protocol::{client::KvsClient, GetRequest, RemoveRequest, SetRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let addr: String = opt
        .addr
        .unwrap_or_else(|| "http://127.0.0.1:4000".to_owned());
    let mut client = KvsClient::connect(addr).await?;

    match opt.cmd {
        Cmd::Set { key, value } => {
            let req = tonic::Request::new(SetRequest { key, value });
            let resp = client.set(req).await?;
            println!("{:?}", resp);
        }
        Cmd::Get { key } => {
            let req = tonic::Request::new(GetRequest { key });
            let resp = client.get(req).await?;
            println!("{:?}", resp);
        }
        Cmd::Remove { key } => {
            let req = tonic::Request::new(RemoveRequest { key });
            let resp = client.remove(req).await?;
            println!("{:?}", resp);
        }
    };

    Ok(())
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
