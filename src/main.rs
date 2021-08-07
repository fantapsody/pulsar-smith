mod opts;
mod context;
mod cmd;
mod config;
mod admin;
mod auth;

#[macro_use]
extern crate log;

use std::error::Error;
use crate::opts::{parse_opts, Command};
use crate::config::Configs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    env_logger::init();

    let opts = parse_opts();
    let cfg = if let Some(_) = &opts.url {
        opts.to_pulsar_config()
    } else {
        let configs = Configs::load()?;
        if let Some(context_name) = &opts.context {
            configs.get_pulsar_config(context_name)?
        } else if configs.has_current_context() {
            configs.get_current_pulsar_config()?
        } else {
            return Err(Box::<dyn Error>::from("no valid contexts"));
        }
    };

    let mut ctx = cfg.into();
    match opts.cmd {
        Command::Produce(x) => x.run(&mut ctx).await?,
        Command::Consume(x) => x.run(&mut ctx).await?,
        Command::Tenants(x) => x.run(&mut ctx).await?,
        Command::Topics(x) => x.run(&mut ctx).await?,
        Command::Auth(x) => x.run(&mut ctx).await?,
    };

    Ok(())
}
