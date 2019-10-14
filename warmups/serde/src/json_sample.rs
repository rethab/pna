use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::Path;

use self::MError::*;

#[derive(Debug)]
pub enum MError {
    Serialization(serde_json::Error),
    IO(std::io::Error),
}

impl Display for MError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Serialization(underlying) => {
                write!(f, "Serialization Error: {}", underlying.description())
            }
            IO(underlying) => write!(f, "IO Error: {}", underlying.description()),
        }
    }
}

impl Error for MError {}

impl From<serde_json::Error> for MError {
    fn from(s_error: serde_json::Error) -> MError {
        MError::Serialization(s_error)
    }
}

impl From<std::io::Error> for MError {
    fn from(std_error: std::io::Error) -> MError {
        MError::IO(std_error)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Move {
    steps: u32,
    direction: Direction,
}

impl Move {
    pub fn new() -> Move {
        Move {
            steps: 12,
            direction: Direction::Left,
        }
    }
}

pub fn write_to_file(m: &Move, file_name: &Path) -> Result<usize, MError> {
    let contents = serde_json::to_string(m)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(file_name)?;
    let bytes = contents.as_bytes();
    file.write_all(bytes)?;
    Ok(bytes.len())
}

pub fn read_from_file(file_name: &Path) -> Option<Move> {
    let mut file = File::open(file_name).unwrap();
    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();
    serde_json::from_str(&buffer).ok()
}
