#[macro_use]
extern crate log;

use crate::cmd::cmd::AsyncCmd;
use crate::config::Configs;
use crate::error::Error;
use crate::opts::{Command, parse_opts};

mod opts;
mod context;
mod cmd;
mod config;
mod admin;
mod auth;
pub mod error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let opts = parse_opts();
    let cfg = if let Some(_) = &opts.url {
        opts.to_pulsar_config()
    } else if matches!(opts.cmd, Command::Auth(_)) && opts.auth_name.is_some() && opts.auth_params.is_some() {
        opts.to_pulsar_config()
    } else {
        let configs = Configs::load()?;
        if let Some(context_name) = &opts.context {
            configs.get_pulsar_config(context_name)?
        } else if configs.has_current_context() {
            configs.get_current_pulsar_config()?
        } else {
            return Err(Error::Custom("no valid contexts".to_string()));
        }
    };

    let mut ctx = cfg.into();
    let cmd: &dyn AsyncCmd = match &opts.cmd {
        Command::Produce(x) => x,
        Command::Consume(x) => x,
        Command::Clusters(x) => x,
        Command::Tenants(x) => x,
        Command::Namespaces(x) => x,
        Command::Topics(x) => x,
        Command::Auth(x) => x,
    };
    cmd.run(&mut ctx).await?;

    Ok(())
}
