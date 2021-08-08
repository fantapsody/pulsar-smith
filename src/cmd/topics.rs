use std::error::Error;

use clap::Clap;

use crate::admin::topics::TopicDomain;
use crate::context::PulsarContext;

#[derive(Clap, Debug, Clone)]
pub struct TopicsOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl TopicsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        match &self.cmd {
            Command::List(opts) => opts.run(pulsar_ctx).await?,
            Command::Lookup(opts) => opts.run(pulsar_ctx).await?,
            Command::Stats(opts) => opts.run(pulsar_ctx).await?,
        }
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    Lookup(LookupOpts),
    Stats(StatsOpts),
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

#[derive(Clap, Debug, Clone)]
pub struct LookupOpts {
    pub topic: String,
}

impl LookupOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .topics()
            .lookup(self.topic.as_str())
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct StatsOpts {
    pub topic: String,

    #[clap(long)]
    get_precise_backlog: bool,

    #[clap(long)]
    subscription_backlog_size: bool,
}

impl StatsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .topics()
            .stats(self.topic.as_str(), self.get_precise_backlog, self.subscription_backlog_size)
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}
