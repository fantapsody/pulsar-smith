mod opts;
mod context;
mod cmd;

#[macro_use]
extern crate log;

use std::error::Error;
use crate::opts::{parse_opts, Command};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    env_logger::init();

    let opts = parse_opts();

    let mut ctx = opts.create_context();
    match opts.cmd {
        Command::Produce(x) => x.run(&mut ctx).await?,
        Command::Consume(x) => x.run(&mut ctx).await?,
    };

    Ok(())
}
