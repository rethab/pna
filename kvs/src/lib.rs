#![deny(missing_docs)]

//! A key value store implementation for the course Practical Networked Applications from PingCAP
//!
//! # Examples
//!
//! ```
//!  # use kvs::KvStore;
//!  let mut kv = KvStore::new();
//!  kv.set(String::from("foo"), String::from("bar"));
//!  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")));
//!  kv.remove(String::from("foo"));
//!  assert_eq!(None, kv.get(String::from("foo")));
//! ```

use failure::Fail;
use serde::{Deserialize, Serialize};
use serde_json;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::path::Path;

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
        }
    }
}

impl From<io::Error> for KvError {
    fn from(io: io::Error) -> KvError {
        KvError::IOError { cause: io }
    }
}

impl From<serde_json::error::Error> for KvError {
    fn from(ser: serde_json::error::Error) -> KvError {
        KvError::SerializationError { cause: ser }
    }
}

/// Result type for all operations in this library
pub type Result<T> = std::result::Result<T, KvError>;

/// A simple key value store
pub struct KvStore {
    file: File,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },

    Remove { key: String },
}

impl KvStore {
    /// Creates a key value store in the specified directory
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::open(Path::new("/tmp/"));
    /// ```
    pub fn open(path: &Path) -> Result<KvStore> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("db"))?;
        Ok(KvStore { file })
    }

    /// Adds a new key-value mapping to the store
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::new();
    ///  kv.set(String::from("foo"), String::from("bar"));
    ///  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")));
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key, value };
        self.append(&cmd)
    }

    /// Returns the value associated with the specified key
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::new();
    ///  kv.set(String::from("foo"), String::from("bar"));
    ///  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")));
    /// ```
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut result = None;
        let stream = serde_json::Deserializer::from_reader(&self.file).into_iter::<Command>();
        for cmd in stream {
            use Command::*;
            match cmd? {
                Set { key, value } if k == key => result = Some(value),
                Remove { key } if k == key => result = None,
                _ => {}
            }
        }
        Ok(result)
    }

    /// Removes the value associated with the specified key
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::new();
    ///  kv.set(String::from("foo"), String::from("bar"));
    ///  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")));
    ///  kv.remove(String::from("foo"));
    ///  assert_eq!(None, kv.get(String::from("foo")));
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.get(key.clone())? {
            None => Err(KvError::KeyNotFound),
            Some(_) => {
                let cmd = Command::Remove { key };
                self.append(&cmd)
            }
        }
    }

    fn append(&mut self, cmd: &Command) -> Result<()> {
        let contents = serde_json::to_string(cmd)?;
        let bytes = contents.as_bytes();
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(bytes)?;
        Ok(())
    }
}
