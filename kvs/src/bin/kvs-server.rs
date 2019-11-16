use std::str::FromStr;
use structopt::StructOpt;

fn main() {
    let opt = Opt::from_args();

    let _engine = opt.engine.unwrap_or_else({ || Engine::Kvs });
    let _addr = opt.addr.unwrap_or_else({ || "127.0.0.1:4000".to_owned() });
}

#[derive(Debug, StructOpt)]
#[structopt(about = "The server of a simple key value store")]
struct Opt {
    // The IP:PORT where the server should bind to. Defaults to 127.0.0.1:4000
    #[structopt(long)]
    addr: Option<String>,

    // The storage engine to use. Can be either 'kvs' or 'sled'
    #[structopt(long)]
    engine: Option<Engine>,
}

#[derive(Debug)]
enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    type Err = String;
    fn from_str(s: &str) -> Result<Engine, String> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            other => Err(format!("Engine '{}' does not exist", other)),
        }
    }
}
