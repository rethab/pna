#![deny(missing_docs)]
#![feature(seek_convenience)]

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
use std::collections::HashMap;
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

struct ValueOffset(u64);

/// A simple key value store
pub struct KvStore {
    file: File,
    values: HashMap<String, ValueOffset>,
}

#[derive(Deserialize, Serialize, Debug)]
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
        let values = KvStore::read_log(&file)?;
        Ok(KvStore { file, values })
    }

    fn read_log(mut db: &File) -> Result<HashMap<String, ValueOffset>> {
        let mut values = HashMap::new();

        let mut pos = db.stream_position()?;
        let mut stream = serde_json::Deserializer::from_reader(db).into_iter::<Command>();
        while let Some(cmd) = stream.next() {
            match cmd? {
                Command::Set { key, .. } => values.insert(key, ValueOffset(pos)),
                Command::Remove { key } => values.remove(&key),
            };
            pos = stream.byte_offset() as u64;
        }
        Ok(values)
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
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let offset = self.append(&cmd)?;
        self.values.insert(key, offset);
        Ok(())
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
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let file = &self.file;
        match self.values.get(&key) {
            None => Ok(None),
            Some(offset) => Ok(Some(KvStore::read_at_offset(file, offset)?)),
        }
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
        match self.values.remove(&key) {
            None => Err(KvError::KeyNotFound),
            Some(_) => {
                let cmd = Command::Remove { key };
                self.append(&cmd)?;
                Ok(())
            }
        }
    }

    fn read_at_offset(mut file: &File, offset: &ValueOffset) -> Result<String> {
        file.seek(SeekFrom::Start(offset.0))?;
        let maybe_cmd = serde_json::Deserializer::from_reader(file)
            .into_iter::<Command>()
            .next();
        match maybe_cmd {
            Some(Ok(Command::Set { value, .. })) => Ok(value),
            _ => Err(KvError::Consistency(format!(
                "No 'Set' command at offset {}",
                offset.0
            ))),
        }
    }

    fn append(&mut self, cmd: &Command) -> Result<ValueOffset> {
        let contents = serde_json::to_string(cmd)?;
        let bytes = contents.as_bytes();
        self.file.seek(SeekFrom::End(0))?;
        let offset = ValueOffset(self.file.stream_position()?);
        self.file.write_all(bytes)?;
        Ok(offset)
    }
}
