use async_trait::async_trait;
use clap::Parser;

use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Parser, Debug, Clone)]
pub struct ClustersOpts {
    #[command(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for ClustersOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd: &dyn AsyncCmd = match &self.cmd {
            Command::List(opts) => opts,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Parser, Debug, Clone)]
pub enum Command {
    List(ListOpts),
}

#[derive(Parser, Debug, Clone)]
pub struct ListOpts {}

#[async_trait]
impl AsyncCmd for ListOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .clusters()
            .list()
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
