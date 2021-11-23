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
            Command::Create(opts) => opts.run(pulsar_ctx).await?,
            Command::Delete(opts) => opts.run(pulsar_ctx).await?,
            Command::Lookup(opts) => opts.run(pulsar_ctx).await?,
            Command::Stats(opts) => opts.run(pulsar_ctx).await?,
            Command::Permissions(opts) => opts.run(pulsar_ctx).await?,
            Command::GrantPermissions(opts) => opts.run(pulsar_ctx).await?,
            Command::RevokePermissions(opts) => opts.run(pulsar_ctx).await?,
        }
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    Create(CreateTopicOpts),
    Delete(DeleteTopicOpts),
    Lookup(LookupOpts),
    Stats(StatsOpts),
    Permissions(PermissionsOpts),
    GrantPermissions(GrantPermissionsOpts),
    RevokePermissions(RevokePermissionsOpts),
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
pub struct CreateTopicOpts {
    pub topic: String,

    #[clap(short = 'p', long, default_value = "0")]
    pub partitions: i32,
}

impl CreateTopicOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        if self.partitions == 0 {
            pulsar_ctx.admin().await?
                .topics()
                .create_non_partitioned_topic(self.topic.as_str())
                .await?;
            Ok(())
        } else if self.partitions > 0 {
            pulsar_ctx.admin().await?
                .topics()
                .create_partitioned_topic(self.topic.as_str(), self.partitions)
                .await?;
            Ok(())
        } else {
            Err(Box::from(format!("invalid partitions [{}]", self.partitions)))
        }
    }
}

#[derive(Clap, Debug, Clone)]
pub struct DeleteTopicOpts {
    pub topic: String,

    #[clap(short = 'f', long)]
    pub force: bool,

    #[clap(short = 'd', long)]
    pub delete_schema: bool,
}

impl DeleteTopicOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        pulsar_ctx.admin().await?
            .topics()
            .delete_topic(self.topic.as_str(), self.force, self.delete_schema)
            .await?;
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

#[derive(Clap, Debug, Clone)]
pub struct PermissionsOpts {
    pub topic: String,
}

impl PermissionsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .topics()
            .permissions(self.topic.as_str())
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}


#[derive(Clap, Debug, Clone)]
pub struct GrantPermissionsOpts {
    pub topic: String,

    #[clap(long)]
    pub role: String,

    #[clap(long)]
    pub actions: Vec<String>,
}

impl GrantPermissionsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        pulsar_ctx.admin().await?
            .topics()
            .grant_permissions(self.topic.as_str(), self.role.as_str(), &self.actions)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct RevokePermissionsOpts {
    pub topic: String,

    #[clap(long)]
    pub role: String,
}

impl RevokePermissionsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        pulsar_ctx.admin().await?
            .topics()
            .revoke_permissions(self.topic.as_str(), self.role.as_str())
            .await?;
        Ok(())
    }
}
