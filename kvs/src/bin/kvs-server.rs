extern crate kvs;
use std::str::FromStr;
use structopt::StructOpt;
use std::path::Path;
use tonic::{transport::Server, Request, Response, Status, Code};
use kvs::KvStore;

mod protocol {
    tonic::include_proto!("kvs");
}

use protocol::{
    server::{Kvs, KvsServer},
    GetReply, GetRequest,
    SetRequest, SetReply,
    RemoveRequest, RemoveReply,
};

pub struct MyKvsServer { }

impl MyKvsServer {
  fn kverror_to_status(kve: kvs::KvError) -> Status {
    Status::new(Code::Internal, format!("{:?}", kve))
  }

  fn kv(&self) -> Result<KvStore, Status> {
    Ok(KvStore::open(Path::new(".")).map_err(MyKvsServer::kverror_to_status)?)
  }
}

#[tonic::async_trait]
impl Kvs for MyKvsServer {
  async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
    let kv = self.kv()?;
    match kv.get(request.into_inner().key).map_err(MyKvsServer::kverror_to_status)? {
      Some(value) => Ok(Response::new(GetReply { value })),
      None => Ok(Response::new(GetReply { value: "arghhhh".to_owned() })),
    }
  }

  async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetReply>, Status> {
    let mut kv = self.kv()?;
    let req = request.into_inner();
    kv.set(req.key, req.value).map_err(MyKvsServer::kverror_to_status)?;
    Ok(Response::new(SetReply{}))
  }

  async fn remove(&self, request: Request<RemoveRequest>) -> Result<Response<RemoveReply>, Status> {
    let mut kv = self.kv()?;
    kv.remove(request.into_inner().key).map_err(MyKvsServer::kverror_to_status)?;
    Ok(Response::new(RemoveReply{}))
  }

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let _engine = opt.engine.unwrap_or_else({ || Engine::Kvs });
    let addr = opt.addr.unwrap_or_else({ || "127.0.0.1:4000".to_owned() }).parse()?;

    let dummy = MyKvsServer{};

    Server::builder()
        .add_service(KvsServer::new(dummy))
        .serve(addr)
        .await?;

    Ok(())
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
