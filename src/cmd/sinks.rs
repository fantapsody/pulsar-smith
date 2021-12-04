use async_trait::async_trait;
use clap::Clap;

use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct SinksOpts {
    #[clap(subcommand)]
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

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    AvailableSinks(AvailableSinksOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct ListOpts {
    #[clap(long)]
    pub tenant: String,

    #[clap(long)]
    pub namespace: String,
}

#[async_trait]
impl AsyncCmd for ListOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin()
            .sinks()
            .list(format!("{}/{}", self.tenant, self.namespace).as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct AvailableSinksOpts {}

#[async_trait]
impl AsyncCmd for AvailableSinksOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin()
            .sinks()
            .builtin_sinks()
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
