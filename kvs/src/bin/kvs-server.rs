extern crate kvs;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use crate::slog::Drain;
use kvs::engine::{KvError, KvsEngine};
use kvs::store::KvStore;
use slog::Logger;
use std::path::Path;
use std::str::FromStr;
use structopt::StructOpt;
use tonic::{transport::Server, Code, Request, Response, Status};

mod protocol {
    tonic::include_proto!("kvs");
}

use protocol::{
    server::{Kvs, KvsServer},
    GetReply, GetRequest, RemoveReply, RemoveRequest, SetReply, SetRequest, Value,
};

pub struct MyKvsServer {
    logger: Logger,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let _engine = opt.engine.unwrap_or_else({ || Engine::Kvs });
    let addr = opt
        .addr
        .unwrap_or_else({ || "127.0.0.1:4000".to_owned() })
        .parse()?;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let root = slog::Logger::root(drain, o!());

    let server_logger = root.new(o!("component" => "server"));
    info!(server_logger, "started at {}", addr);

    let server = MyKvsServer::new(root);

    Server::builder()
        .add_service(KvsServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

impl MyKvsServer {
    fn new(logger: Logger) -> MyKvsServer {
        MyKvsServer { logger }
    }

    fn kverror_to_status(kve: KvError) -> Status {
        Status::new(Code::Internal, format!("{:?}", kve))
    }

    fn kv(&self) -> Result<KvStore, Status> {
        let engine_logger = self.logger.new(o!("component" => "store"));
        let store = KvStore::open(Path::new("."), engine_logger);
        Ok(store.map_err(MyKvsServer::kverror_to_status)?)
    }
}

#[tonic::async_trait]
impl Kvs for MyKvsServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let mut kv = self.kv()?;
        let mb_value = kv
            .get(request.into_inner().key)
            .map_err(MyKvsServer::kverror_to_status)?;
        match mb_value {
            Some(value) => Ok(Response::new(GetReply {
                value: Some(Value { value }),
            })),
            None => Ok(Response::new(GetReply { value: None })),
        }
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetReply>, Status> {
        let mut kv = self.kv()?;
        let req = request.into_inner();
        kv.set(req.key, req.value)
            .map_err(MyKvsServer::kverror_to_status)?;
        Ok(Response::new(SetReply {}))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveReply>, Status> {
        let mut kv = self.kv()?;
        kv.remove(request.into_inner().key)
            .map_err(MyKvsServer::kverror_to_status)?;
        Ok(Response::new(RemoveReply {}))
    }
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
