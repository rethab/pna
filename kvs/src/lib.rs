#![deny(missing_docs)]
#![feature(seek_convenience)]

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

// controls whether some functions write debug information
const DEBUG: bool = false;

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
struct Version(u64);

struct ValuePointer {
    file: Rc<File>,
    offset: ValueOffset,
    // the version based on which we can know whether
    // the command in the file is outdated and can be
    // cleaned up: we stoe the version here and in the
    // command. when reading a command, we can compare
    // it with this one and discard the command if that
    // value is lower. the version is increased when ever
    // we add a command by the same key (rm + set)
    version: Version,
}

/// a pluggable storage engine for this kv store
pub trait KvsEngine {}

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
    // number of immutable db files since last compaction
    // the idea is that we increase this counter whenever
    // a new immutable file is created. in the beginning,
    // this is set to zero regardless of the number of
    // immutables
    immutables_since_last_compaction: usize,

    values: HashMap<String, ValuePointer>,
}

impl fmt::Display for KvStore {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "active_for_read:{:?}", self.active_for_read)?;
        write!(fmt, "active_for_write:{:?}", self.active_for_write)?;
        for (k, v) in &self.values {
            write!(
                fmt,
                "{}: offset={}, version={}, file={:?}",
                k, v.offset.0, v.version.0, v.file
            )?;
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
enum Command {
    Set {
        key: String,
        value: String,
        version: u64,
    },

    Remove {
        key: String,
    },
}

fn debug(msg: String) {
    if DEBUG {
        println!("{}", msg)
    }
}

// helper type for the function read_logs that returns information about a log file
struct LogValues {
    // the keys in this file (w/o the ones that were
    // removed (a 'Set' command followed by an 'Remove' command)
    values: HashMap<String, (ValueOffset, Version)>,
    // the number of elements in this file
    size: usize,
}

impl KvStore {
    const ACTIVE_FILE_NAME: &'static str = "db.active";

    // after the active log file has reached this number
    // of entries, we rotate the file. note that this is
    // small (for now) for testing.
    const FILE_ROTATION_TRESHOLD: usize = 150;

    // after the number of files has reached this number,
    // we run the compaction trying to reduce the number
    // of files again
    const COMPACTION_TRESHOLD: usize = 5;

    /// Creates a key value store in the specified directory
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  # use std::path::Path;
    ///  let mut kv = KvStore::open(Path::new("/tmp/"));
    /// ```
    pub fn open(dir: &Path) -> Result<KvStore> {
        let (mut values, highest_counter) = KvStore::read_immutable_logs(&dir)?;

        let active_path = dir.join(KvStore::ACTIVE_FILE_NAME);
        let active_for_read = Rc::new(
            // must be create+write or it will fail on the first call
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&active_path)?,
        );
        let LogValues {
            values: offsets,
            size,
        } = KvStore::read_log(&active_for_read)?;
        for (key, (offset, version)) in offsets {
            let active_for_read = active_for_read.clone();
            values.insert(
                key,
                ValuePointer {
                    file: active_for_read,
                    offset,
                    version,
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
            active_for_read,
            active_entries: size,
            immutable_counter: highest_counter,
            immutables_since_last_compaction: 0,
            values,
        })
    }

    fn is_immutable_file(path: &Path) -> bool {
        path.extension()
            .map(|extension| extension.to_string_lossy() == "immutable")
            .unwrap_or_else(|| false)
    }

    fn read_immutable_logs(dir: &Path) -> Result<(HashMap<String, ValuePointer>, u64)> {
        let mut values = HashMap::new();
        let mut highest_counter = 0;
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if KvStore::is_immutable_file(&path) {
                let counter = KvStore::extract_counter(&path)?;
                if counter > highest_counter {
                    highest_counter = counter;
                }

                let file = Rc::new(OpenOptions::new().read(true).open(path)?);
                for (key, (offset, version)) in KvStore::read_log(&file)?.values {
                    let file = file.clone();
                    values.insert(
                        key,
                        ValuePointer {
                            file,
                            offset,
                            version,
                        },
                    );
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

    fn read_log(mut file: &File) -> Result<LogValues> {
        let mut offset = file.stream_position()?;
        let mut stream = serde_json::Deserializer::from_reader(file).into_iter::<Command>();
        let mut values = HashMap::new();
        let mut size = 0;
        while let Some(cmd) = stream.next() {
            match cmd? {
                Command::Set { key, version, .. } => {
                    values.insert(key, (ValueOffset(offset), Version(version)))
                }
                Command::Remove { key } => values.remove(&key),
            };
            offset = stream.byte_offset() as u64;
            size += 1;
        }
        Ok(LogValues { values, size })
    }

    /// Adds a new key-value mapping to the store
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  # use std::path::Path;
    ///  let mut kv = KvStore::open(Path::new("/tmp")).unwrap();
    ///  kv.set(String::from("foo"), String::from("bar"));
    ///  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")).unwrap());
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let version = self
            .values
            .get(&key)
            .map(|v| v.version.0 + 1)
            .unwrap_or_else(|| 0);
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
            version,
        };
        let offset = self.append(&cmd, false)?;
        // append modifies active_for_read, so this must happen after
        let file = self.active_for_read.clone();

        let value_pointer = ValuePointer {
            file,
            offset,
            version: Version(version),
        };
        self.values.insert(key, value_pointer);
        Ok(())
    }

    /// Returns the value associated with the specified key
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  # use std::path::Path;
    ///  let mut kv = KvStore::open(Path::new("/tmp")).unwrap();
    ///  kv.set(String::from("foo"), String::from("bar"));
    ///  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")).unwrap());
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.values.get(&key) {
            None => Ok(None),
            Some(ValuePointer { file, offset, .. }) => {
                Ok(Some(KvStore::read_at_offset(file, offset)?))
            }
        }
    }

    /// Removes the value associated with the specified key
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  # use std::path::Path;
    ///  let mut kv = KvStore::open(Path::new("/tmp")).unwrap();
    ///  kv.set(String::from("foo"), String::from("bar"));
    ///  assert_eq!(Some(String::from("bar")), kv.get(String::from("foo")).unwrap());
    ///  kv.remove(String::from("foo"));
    ///  assert_eq!(None, kv.get(String::from("foo")).unwrap());
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.values.remove(&key) {
            None => Err(KvError::KeyNotFound),
            Some(_) => {
                let cmd = Command::Remove { key };
                self.append(&cmd, false)?;
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

    // rotates the active file by renaming the currently
    // active file to immutable.X and creating a new
    // active file
    fn rotate(&mut self) -> Result<()> {
        debug("Rotating".to_owned());
        self.immutable_counter += 1;
        self.immutables_since_last_compaction += 1;
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

    // Compaction Algorithm
    //
    // We iterate through the immutable files in order of
    // their creation. Per file, we keep the active values
    // ('Set' commands we encounter) in a map. If at least
    // one value is dropped (ie. seen, but not part of the
    // active ones), all active ones are appended and the
    // file is unlinked.
    //
    // Whenever we see a 'Remove' command, we can directly
    // see this as a command to be dropped, because if there
    // was a preceding 'Set', it would already have been
    // added to the values to be discarded. Because we
    // process files in the order of their creation, this
    // even works across files, because 'Set' from previous
    // files would have been dropped already.
    //
    // This presupposes that we set the treshold of "unused"
    // values to 0 though. If that was bigger, then we could
    // have an unused 'Set' that was later not discarded,
    // because it was the only obsolete command in a file,
    // while the 'Remove' in the next file would be removed.
    fn compact(&mut self) -> Result<()> {
        debug("Compacting".to_owned());
        let mut immutables: Vec<PathBuf> = fs::read_dir(&self.db_dir)?
            .filter_map(|p| p.ok())
            .map(|e| e.path())
            .filter(|p| KvStore::is_immutable_file(&p))
            .collect();
        immutables.sort();
        for path in immutables {
            self.compact_file(&path)?;
        }
        self.immutables_since_last_compaction = 0;
        Ok(())
    }

    fn compact_file(&mut self, path: &Path) -> Result<()> {
        debug(format!("Compacting file {}", path.to_string_lossy()));
        let mut active_values = HashMap::new();
        let mut inactive_amount = 0;

        let file = OpenOptions::new().read(true).open(path)?;
        let stream = serde_json::Deserializer::from_reader(file).into_iter::<Command>();

        for cmd in stream {
            let cmd = cmd?;
            match cmd {
                Command::Set {
                    ref key,
                    ref version,
                    ..
                } => match self.values.get(key) {
                    Some(value) if *version == value.version.0 => {
                        // debug(format!("Retaining {}, because version matches", key));
                        active_values.insert(key.clone(), cmd);
                    }
                    _ => inactive_amount += 1,
                },
                Command::Remove { .. } => inactive_amount += 1,
            };
        }

        if inactive_amount > 0 {
            debug(format!(
                "Inactive amount: {}, values: {}",
                inactive_amount,
                active_values.len()
            ));
            for (_, cmd) in active_values {
                self.append(&cmd, true)?;
            }
            fs::remove_file(&path)?;
        }

        Ok(())
    }

    fn should_compact(&self) -> bool {
        self.immutables_since_last_compaction >= KvStore::COMPACTION_TRESHOLD
    }

    // no_compaction is used during compaction, when we append but we know that
    // running compaction won't help (and could in fact introduce a loop)
    fn append(&mut self, cmd: &Command, no_compaction: bool) -> Result<ValueOffset> {
        if self.active_entries >= KvStore::FILE_ROTATION_TRESHOLD {
            self.rotate()?;
        }
        if !no_compaction && self.should_compact() {
            self.compact()?;
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
        Ok(offset)
    }
}
