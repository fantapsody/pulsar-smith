use async_trait::async_trait;

use crate::error::Error;
use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use clap::Clap;

#[derive(Clap, Debug, Clone)]
pub struct FunctionOpts {
    #[clap(subcommand)]
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

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
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
        let r = pulsar_ctx.admin().await?
            .functions()
            .list(format!("{}/{}", self.tenant, self.namespace).as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
