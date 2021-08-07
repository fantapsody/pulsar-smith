use std::error::Error;

use clap::Clap;

use crate::cmd::topics::Command::List;
use crate::context::PulsarContext;
use crate::admin::topics::TopicDomain;

#[derive(Clap, Debug, Clone)]
pub struct TopicsOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl TopicsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        match &self.cmd {
            List(opts) => opts.run(pulsar_ctx).await?,
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
    pub namespace: String,

    #[clap(short = 'd', long, default_value = "Persistent")]
    pub domain: String,
}

impl ListOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .topics()
            .list(self.namespace.as_str(), TopicDomain::parse(self.domain.as_ref())?)
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
