#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

pub mod engine;
pub mod store;

pub use engine::{KvsEngine, Result};
pub use store::KvStore;
