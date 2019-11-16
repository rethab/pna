#![deny(missing_docs)]

//! A key value store implementation for the course Practical Networked Applications from PingCAP
//!
//! # Examples
//!
//! ```
//!  # use kvs::KvStore;
//!  # use std::path::Path;
//!  let mut kv = KvStore::open(Path::new("/tmp")).unwrap();
//!  kv.set(String::from("foo"), String::from("bar"));
//!  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")).unwrap());
//!  kv.remove(String::from("foo"));
//!  assert_eq!(None, kv.get(String::from("foo")).unwrap());
//! ```
use failure::Fail;
use serde_json;
use std::error::Error;
use std::fmt;
use std::io;

/// Errors reported by this library
#[derive(Debug, Fail)]
pub enum KvError {
    /// Some IO problem
    IOError {
        /// Underlying IO error error that caused this
        cause: io::Error,
    },
    /// Some serialization problem
    SerializationError {
        /// Underlying serde error
        cause: serde_json::error::Error,
    },

    /// Key was not found
    KeyNotFound,

    /// Something with the database seems inconsitent. Could be corrupted file or bug
    Consistency(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use KvError::*;
        match self {
            IOError { cause } => write!(fmt, "IOError: {}", cause.description().to_owned()),
            SerializationError { cause } => write!(
                fmt,
                "SerializationError: {}",
                cause.description().to_owned()
            ),
            KeyNotFound => write!(fmt, "Key not found"),
            Consistency(msg) => write!(fmt, "ConsistencyError: {}", msg),
        }
    }
}

/// Result type for all operations in this library
pub type Result<T> = std::result::Result<T, KvError>;

/// a pluggable storage engine for this kv store
pub trait KvsEngine {
    ///
    fn set(&mut self, key: String, value: String) -> Result<()>;
    ///
    fn get(&mut self, key: String) -> Result<Option<String>>;
    ///
    fn remove(&mut self, key: String) -> Result<()>;
}
