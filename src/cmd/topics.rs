use async_trait::async_trait;
use clap::Clap;

use crate::admin::topics::TopicDomain;
use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct TopicsOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for TopicsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd: &dyn AsyncCmd = match &self.cmd {
            Command::List(opts) => opts,
            Command::Create(opts) => opts,
            Command::Delete(opts) => opts,
            Command::DeletePartitionedTopic(opts) => opts,
            Command::Lookup(opts) => opts,
            Command::Stats(opts) => opts,
            Command::Permissions(opts) => opts,
            Command::GrantPermissions(opts) => opts,
            Command::RevokePermissions(opts) => opts,
            Command::Subscriptions(opts) => opts,
            Command::Unsubscribe(opts) => opts,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    Create(CreateTopicOpts),
    Delete(DeleteTopicOpts),
    DeletePartitionedTopic(DeletePartitionedTopicOpts),
    Lookup(LookupOpts),
    Stats(StatsOpts),
    Permissions(PermissionsOpts),
    GrantPermissions(GrantPermissionsOpts),
    RevokePermissions(RevokePermissionsOpts),
    Subscriptions(SubscriptionsOpts),
    Unsubscribe(UnsubscribeOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct ListOpts {
    pub namespace: String,

    #[clap(short = 'd', long, default_value = "Persistent")]
    pub domain: String,
}

#[async_trait]
impl AsyncCmd for ListOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

#[async_trait]
impl AsyncCmd for CreateTopicOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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
            Err(Error::Custom(format!("invalid partitions [{}]", self.partitions)))
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

#[async_trait]
impl AsyncCmd for DeleteTopicOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .topics()
            .delete_topic(self.topic.as_str(), self.force, self.delete_schema)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct DeletePartitionedTopicOpts {
    pub topic: String,

    #[clap(short = 'f', long)]
    pub force: bool,

    #[clap(short = 'd', long)]
    pub delete_schema: bool,
}

#[async_trait]
impl AsyncCmd for DeletePartitionedTopicOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .topics()
            .delete_partitioned_topic(self.topic.as_str(), self.force, self.delete_schema)
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct LookupOpts {
    pub topic: String,
}

#[async_trait]
impl AsyncCmd for LookupOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

#[async_trait]
impl AsyncCmd for StatsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

#[async_trait]
impl AsyncCmd for PermissionsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

#[async_trait]
impl AsyncCmd for GrantPermissionsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

#[async_trait]
impl AsyncCmd for RevokePermissionsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .topics()
            .revoke_permissions(self.topic.as_str(), self.role.as_str())
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct SubscriptionsOpts {
    pub topic: String,
}

#[async_trait]
impl AsyncCmd for SubscriptionsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let subscriptions = pulsar_ctx.admin().await?
            .topics()
            .subscriptions(self.topic.as_str())
            .await?;
        println!("{}", serde_json::to_string(&subscriptions)?);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct UnsubscribeOpts {
    pub topic: String,

    #[clap(short = 's', long)]
    pub subscription: String,

    #[clap(short = 'f', long)]
    pub force: bool,
}

#[async_trait]
impl AsyncCmd for UnsubscribeOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .topics()
            .unsubscribe(self.topic.as_str(), self.subscription.as_str(), self.force)
            .await?;
        Ok(())
    }
}
