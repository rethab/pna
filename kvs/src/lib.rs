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
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::rc::Rc;

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

struct ValuePointer {
    file: Rc<File>,
    offset: ValueOffset,
}

/// A simple key value store
pub struct KvStore {
    db_dir: PathBuf,
    // we keep two references to the active file,
    // because to modify them (needed for appen), we
    // need a RefCell, but in the values below, we
    // need only the Rc (see ValuePointer). I didn't
    // know how to convert the former to the latter
    active_for_write: Rc<RefCell<File>>,
    active_for_read: Rc<File>,
    // number of values in the active file
    active_entries: usize,
    // current highest value of immutable files
    immutable_counter: u64,
    values: HashMap<String, ValuePointer>,
}

#[derive(Deserialize, Serialize, Debug)]
enum Command {
    Set { key: String, value: String },

    Remove { key: String },
}

impl KvStore {
    const ACTIVE_FILE_NAME: &'static str = "db.active";

    // after the active log file has reached this number
    // of entries, we rotate the file. note that this is
    // small (for now) for testing.
    const FILE_ROTATION_TRESHOLD: usize = 3;

    /// Creates a key value store in the specified directory
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::open(Path::new("/tmp/"));
    /// ```
    pub fn open(dir: &Path) -> Result<KvStore> {
        let (mut values, highest_counter) = KvStore::read_immutable_logs(&dir)?;

        let active_path = dir.join(KvStore::ACTIVE_FILE_NAME);
        let mut active_for_read = Rc::new(
            // must be create+write or it will fail on the first call
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&active_path)?,
        );
        let (offsets, active_entries) = KvStore::read_log(&mut active_for_read)?;
        for (key, offset) in offsets {
            let active_for_read = active_for_read.clone();
            values.insert(
                key,
                ValuePointer {
                    file: active_for_read,
                    offset,
                },
            );
        }

        let active_for_write = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&active_path)?;

        Ok(KvStore {
            db_dir: dir.to_owned(),
            active_for_write: Rc::new(RefCell::new(active_for_write)),
            active_for_read: active_for_read,
            active_entries,
            immutable_counter: highest_counter,
            values,
        })
    }

    fn read_immutable_logs(dir: &Path) -> Result<(HashMap<String, ValuePointer>, u64)> {
        let mut values = HashMap::new();
        let mut highest_counter = 0;
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.to_string_lossy().ends_with(".immutable") {
                let counter = KvStore::extract_counter(&path)?;
                if counter > highest_counter {
                    highest_counter = counter;
                }
                let mut file = Rc::new(OpenOptions::new().read(true).open(path)?);
                for (key, offset) in KvStore::read_log(&mut file)?.0 {
                    let file = file.clone();
                    values.insert(key, ValuePointer { file, offset });
                }
            }
        }
        Ok((values, highest_counter))
    }

    fn extract_counter(path: &Path) -> Result<u64> {
        match path.file_stem() {
            None => Err(KvError::Consistency(format!(
                "No file name: {}",
                path.to_string_lossy()
            ))),
            Some(stem) => {
                let stem = stem.to_string_lossy();
                stem.parse::<u64>()
                    .map_err(|_| KvError::Consistency(format!("Invalid file name: {}", stem)))
            }
        }
    }

    fn read_log(mut file: &File) -> Result<(HashMap<String, ValueOffset>, usize)> {
        let mut offset = file.stream_position()?;
        let mut stream = serde_json::Deserializer::from_reader(file).into_iter::<Command>();
        let mut values = HashMap::new();
        let mut active_entries = 0;
        while let Some(cmd) = stream.next() {
            match cmd? {
                Command::Set { key, .. } => values.insert(key, ValueOffset(offset)),
                Command::Remove { key } => values.remove(&key),
            };
            offset = stream.byte_offset() as u64;
            active_entries += 1;
        }
        Ok((values, active_entries))
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
        let value_pointer = self.append(&cmd)?;
        self.values.insert(key, value_pointer);
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
        match self.values.get(&key) {
            None => Ok(None),
            Some(ValuePointer { file, offset }) => Ok(Some(KvStore::read_at_offset(file, offset)?)),
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

    fn rotate(&mut self) -> Result<()> {
        self.immutable_counter += 1;
        let immutable_file_path = self
            .db_dir
            .join(format!("{}.immutable", self.immutable_counter));
        let active_file_path = self.db_dir.join(KvStore::ACTIVE_FILE_NAME);
        fs::rename(&active_file_path, immutable_file_path)?;

        self.active_for_write = Rc::new(RefCell::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&active_file_path)?,
        ));

        self.active_for_read = Rc::new(
            OpenOptions::new()
                .read(true)
                .create(true)
                .write(true)
                .open(&active_file_path)?,
        );

        self.active_entries = 0;
        Ok(())
    }

    fn append(&mut self, cmd: &Command) -> Result<ValuePointer> {
        if self.active_entries >= KvStore::FILE_ROTATION_TRESHOLD {
            self.rotate()?;
        }
        let contents = serde_json::to_string(cmd)?;
        let bytes = contents.as_bytes();
        let offset = {
            let mut active = self.active_for_write.borrow_mut();
            active.seek(SeekFrom::End(0))?;
            let offset = ValueOffset(active.stream_position()?);
            active.write_all(bytes)?;
            offset
        };
        self.active_entries += 1;
        Ok(ValuePointer {
            file: self.active_for_read.clone(),
            offset,
        })
    }
}
