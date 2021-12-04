use crate::context::PulsarContext;
use crate::error::Error;
use clap::Clap;

#[derive(Clap, Debug, Clone)]
pub struct ClustersOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl ClustersOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        match &self.cmd {
            Command::List(opts) => opts.run(pulsar_ctx).await?,
        }
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct ListOpts {
}

impl ListOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .clusters()
            .list()
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
