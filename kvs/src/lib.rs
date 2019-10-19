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
use std;
use std::collections::HashMap;
use std::path::Path;

/// Errors reported by this library
#[derive(Debug, Fail)]
pub enum Error {
    /// Trying to load a database that doesn't exist
    #[fail(display = "IOError: {}", msg)]
    IOError {
        /// Detailed error message
        msg: String,
    },
}

/// Result type for all operations in this library
pub type Result<T> = std::result::Result<T, Error>;

/// A simple key value store
pub struct KvStore {
    values: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty key value store
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::new();
    /// ```
    pub fn new() -> KvStore {
        KvStore {
            values: HashMap::new(),
        }
    }

    /// Creates a key value store based on an existing database
    ///
    /// # Examples
    ///
    /// ```
    ///  # use kvs::KvStore;
    ///  let mut kv = KvStore::open(Path::new("/tmp/mydb"));
    /// ```
    pub fn open(path: &Path) -> Result<KvStore> {
        unimplemented!();
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
        self.values.insert(key, value);
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
        Ok(self.values.get(&key).map(|v| v.to_string()))
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
        self.values.remove(&key);
        Ok(())
    }
}
