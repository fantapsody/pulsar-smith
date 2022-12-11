use async_trait::async_trait;
use clap::Parser;

use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Parser, Debug, Clone)]
pub struct SinksOpts {
    #[command(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for SinksOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd: &dyn AsyncCmd = match &self.cmd {
            Command::List(opts) => opts,
            Command::AvailableSinks(opts) => opts,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Parser, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    AvailableSinks(AvailableSinksOpts),
}

#[derive(Parser, Debug, Clone)]
pub struct ListOpts {
    #[arg(long)]
    pub tenant: String,

    #[arg(long)]
    pub namespace: String,
}

#[async_trait]
impl AsyncCmd for ListOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .sinks()
            .list(format!("{}/{}", self.tenant, self.namespace).as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Parser, Debug, Clone)]
pub struct AvailableSinksOpts {}

#[async_trait]
impl AsyncCmd for AvailableSinksOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .sinks()
            .builtin_sinks()
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}
