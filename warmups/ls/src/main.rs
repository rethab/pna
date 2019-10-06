#![feature(option_flattening)]
#[macro_use]
extern crate clap;
extern crate chrono;
use clap::App;

use std::fs;
use std::fs::Metadata;
use std::path::PathBuf;
use std::process;

use chrono::offset::Utc;
use chrono::DateTime;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let m = App::from_yaml(yaml).get_matches();

    let path = m.value_of("PATH").unwrap_or(".");
    let long = m.is_present("long");
    let all = m.is_present("all");

    if let Ok(entries) = fs::read_dir(path) {
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|p| all || visible(p))
            .filter_map(|p| show(long, p))
            .for_each(|item| println!("{}", item));
    } else {
        eprintln!("File {} not found", path);
        process::exit(1);
    }
}

// Shows the path according to the preferences
// if `long` is true, long listing format is used
// returns `None` if the path (or its metadata if requested)
// cannot be resolved.
fn show(long: bool, path: PathBuf) -> Option<String> {
    path.file_name()
        .map(|file_name| file_name.to_string_lossy().to_string())
        .and_then(|file_name| {
            if long {
                path.metadata()
                    .map(|meta| format!("{} {}", show_meta(meta), file_name))
                    .ok()
            } else {
                Some(file_name)
            }
        })
}

// Shows some metadata for the file. Note that
// since only little information is available in a platform
// independent way, this function doesn't return a lot.
fn show_meta(meta: Metadata) -> String {
    let ftype = if meta.is_dir() {
        'd'
    } else if meta.file_type().is_symlink() {
        'l'
    } else {
        '-'
    };
    let permissions = if meta.permissions().readonly() {
        'r'
    } else {
        'w'
    };
    let size = meta.len();
    let datetime = meta
        .modified()
        .map(|modified| {
            let datetime: DateTime<Utc> = modified.into();
            format!("{}", datetime.format("%b %d %H:%m"))
        })
        .unwrap_or(String::new());
    format!("{}{} {:>7} {}", ftype, permissions, size, datetime)
}

// returns true if the specified path shall be listed (that is,
// is not a hidden directory)
fn visible(path: &PathBuf) -> bool {
    !path
        .file_name()
        .map(|name| name.to_str())
        .flatten()
        .map(|name| name.starts_with("."))
        .unwrap_or(true)
}
