use std::error::Error;

use clap::Clap;

use crate::context::PulsarContext;

#[derive(Clap, Debug, Clone)]
pub struct TenantsOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl TenantsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
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
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .tenants()
            .list()
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
