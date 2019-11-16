extern crate kvs;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use crate::slog::Drain;
use kvs::engine::{KvError, KvsEngine};
use kvs::store::KvStore;
use std::fmt;
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

pub struct KvsServerImpl {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let engine = opt.engine.unwrap_or_else({ || Engine::Kvs });
    let addr = opt
        .addr
        .unwrap_or_else({ || "127.0.0.1:4000".to_owned() })
        .parse()?;

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let root = slog::Logger::root(drain, o!());

    let server_logger = root.new(o!("component" => "server"));
    info!(
        server_logger,
        "PNA: Rust Key/Value Store ({})",
        env!("CARGO_PKG_VERSION")
    );
    info!(server_logger, "started at {}", addr);
    info!(server_logger, "using storage engine {}", engine);

    let server = KvsServerImpl {};

    Server::builder()
        .add_service(KvsServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}

impl KvsServerImpl {
    fn kverror_to_status(kve: KvError) -> Status {
        Status::new(Code::Internal, format!("{:?}", kve))
    }

    fn kv(&self) -> Result<KvStore, Status> {
        let store = KvStore::open(Path::new("."));
        Ok(store.map_err(KvsServerImpl::kverror_to_status)?)
    }
}

#[tonic::async_trait]
impl Kvs for KvsServerImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let mut kv = self.kv()?;
        let mb_value = kv
            .get(request.into_inner().key)
            .map_err(KvsServerImpl::kverror_to_status)?;
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
            .map_err(KvsServerImpl::kverror_to_status)?;
        Ok(Response::new(SetReply {}))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveReply>, Status> {
        let mut kv = self.kv()?;
        match kv.remove(request.into_inner().key) {
            Ok(()) => Ok(Response::new(RemoveReply { removed: true })),
            Err(KvError::KeyNotFound) => Ok(Response::new(RemoveReply { removed: false })),
            Err(other) => Err(KvsServerImpl::kverror_to_status(other)),
        }
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

impl fmt::Display for Engine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Engine::Kvs => write!(fmt, "kvs"),
            Engine::Sled => write!(fmt, "sled"),
        }
    }
}
