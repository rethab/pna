extern crate clap;
extern crate kvs;

use clap::{App, Arg, SubCommand};
use kvs::KvStore;
use std::process;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("sets a value")
                .arg(Arg::with_name("key").value_name("KEY").required(true))
                .arg(Arg::with_name("value").value_name("VALUE").required(true)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("gets a value")
                .arg(Arg::with_name("key").value_name("KEY").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("removes a value")
                .arg(Arg::with_name("key").value_name("KEY").required(true)),
        )
        .get_matches();

    let mut kv = KvStore::new();
    if let Some(set_cmd) = matches.subcommand_matches("set") {
        let key = set_cmd.value_of("key").unwrap().to_owned();
        let value = set_cmd.value_of("value").unwrap().to_owned();
        panic!("unimplemented");
    } else if let Some(get_cmd) = matches.subcommand_matches("get") {
        let key = get_cmd.value_of("key").unwrap().to_owned();
        panic!("unimplemented");
    } else if let Some(rm_cmd) = matches.subcommand_matches("rm") {
        let key = rm_cmd.value_of("key").unwrap().to_owned();
        panic!("unimplemented");
    } else if !matches.is_present("version") {
        eprintln!("missing command");
        println!("{}", matches.usage());
        process::exit(1);
    }
}
