use async_trait::async_trait;
use clap::Clap;

use crate::admin::namespaces::{NamespacePolicies, PersistencePolicies};
use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct NamespacesOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for NamespacesOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd: &dyn AsyncCmd = match &self.cmd {
            Command::List(opts) => opts,
            Command::Create(opts) => opts,
            Command::Policies(opts) => opts,
            Command::Permissions(opts) => opts,
            Command::GrantPermission(opts) => opts,
            Command::RevokePermission(opts) => opts,
            Command::GetPersistence(opts) => opts,
            Command::SetPersistence(opts) => opts,
            Command::RemovePersistence(opts) => opts,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    Create(CreateOpts),
    Policies(PoliciesOpts),
    Permissions(PermissionsOpts),
    GrantPermission(GrantPermissionOpts),
    RevokePermission(RevokePermissionOpts),
    GetPersistence(GetPersistenceOpts),
    SetPersistence(SetPersistenceOpts),
    RemovePersistence(RemovePersistenceOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct ListOpts {
    tenant: String,
}

#[async_trait]
impl AsyncCmd for ListOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .list(self.tenant.as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct CreateOpts {
    namespace: String,

    #[clap(short = 'b', long)]
    bundles: Option<u64>,

    #[clap(short = 'c', long)]
    clusters: Vec<String>,
}

impl From<&CreateOpts> for NamespacePolicies {
    fn from(opts: &CreateOpts) -> Self {
        NamespacePolicies {
            bundles: None,
            replication_clusters: Some(opts.clusters.clone()),
            ..Default::default()
        }
    }
}

#[async_trait]
impl AsyncCmd for CreateOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .create(self.namespace.as_str(), &self.into())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct PoliciesOpts {
    namespace: String,
}

#[async_trait]
impl AsyncCmd for PoliciesOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .policies(self.namespace.as_str())
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct PermissionsOpts {
    namespace: String,
}

#[async_trait]
impl AsyncCmd for PermissionsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .permissions(self.namespace.as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct GrantPermissionOpts {
    namespace: String,

    #[clap(long)]
    role: String,

    #[clap(long)]
    actions: Vec<String>,
}

#[async_trait]
impl AsyncCmd for GrantPermissionOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .grant_permission(self.namespace.as_str(), self.role.as_str(), &self.actions)
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct RevokePermissionOpts {
    namespace: String,

    #[clap(long)]
    role: String,
}

#[async_trait]
impl AsyncCmd for RevokePermissionOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .revoke_permission(self.namespace.as_str(), self.role.as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct GetPersistenceOpts {
    namespace: String,
}

#[async_trait]
impl AsyncCmd for GetPersistenceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .policies(self.namespace.as_str())
            .await?
            .persistence;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct SetPersistenceOpts {
    namespace: String,

    #[clap(short = 'a', long, default_value = "0")]
    bookkeeper_ack_quorum: i32,

    #[clap(short = 'w', long, default_value = "0")]
    bookkeeper_write_quorum: i32,

    #[clap(short = 'e', long, default_value = "0")]
    bookkeeper_ensemble: i32,

    #[clap(short = 'r', long, default_value = "0")]
    managed_ledger_max_mark_delete_rate: f64,
}

impl From<&SetPersistenceOpts> for PersistencePolicies {
    fn from(opts: &SetPersistenceOpts) -> Self {
        PersistencePolicies {
            bookkeeper_ensemble: opts.bookkeeper_ensemble,
            bookkeeper_write_quorum: opts.bookkeeper_write_quorum,
            bookkeeper_ack_quorum: opts.bookkeeper_ack_quorum,
            managed_ledger_max_mark_delete_rate: opts.managed_ledger_max_mark_delete_rate,
        }
    }
}

#[async_trait]
impl AsyncCmd for SetPersistenceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .namespaces()
            .update_persistence(self.namespace.as_str(), &self.into())
            .await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct RemovePersistenceOpts {
    namespace: String,
}

#[async_trait]
impl AsyncCmd for RemovePersistenceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .namespaces()
            .remove_persistence(self.namespace.as_str())
            .await?;
        Ok(())
    }
}
