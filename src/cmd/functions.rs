use async_trait::async_trait;

use crate::error::Error;
use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct FunctionOpts {
    #[command(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for FunctionOpts {
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
            .functions()
            .list(format!("{}/{}", self.tenant, self.namespace).as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
