use crate::error::Error;

use clap::Clap;

use crate::context::PulsarContext;
use crate::admin::namespaces::{NamespacePolicies, PersistencePolicies};

#[derive(Clap, Debug, Clone)]
pub struct NamespacesOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl NamespacesOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        match &self.cmd {
            Command::List(opts) => opts.run(pulsar_ctx).await?,
            Command::Create(opts) => opts.run(pulsar_ctx).await?,
            Command::Policies(opts) => opts.run(pulsar_ctx).await?,
            Command::Permissions(opts) => opts.run(pulsar_ctx).await?,
            Command::GrantPermission(opts) => opts.run(pulsar_ctx).await?,
            Command::RevokePermission(opts) => opts.run(pulsar_ctx).await?,
            Command::GetPersistence(opts) => opts.run(pulsar_ctx).await?,
            Command::SetPersistence(opts) => opts.run(pulsar_ctx).await?,
            Command::RemovePersistence(opts) => opts.run(pulsar_ctx).await?,
        }
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

impl ListOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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
        NamespacePolicies{
            bundles: None,
            replication_clusters: Some(opts.clusters.clone()),
            ..Default::default()
        }
    }
}

impl CreateOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

impl PoliciesOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

impl PermissionsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

impl GrantPermissionOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

impl RevokePermissionOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

impl GetPersistenceOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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
        PersistencePolicies{
            bookkeeper_ensemble: opts.bookkeeper_ensemble,
            bookkeeper_write_quorum: opts.bookkeeper_write_quorum,
            bookkeeper_ack_quorum: opts.bookkeeper_ack_quorum,
            managed_ledger_max_mark_delete_rate: opts.managed_ledger_max_mark_delete_rate,
        }
    }
}

impl SetPersistenceOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
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

impl RemovePersistenceOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        pulsar_ctx.admin().await?
            .namespaces()
            .remove_persistence(self.namespace.as_str())
            .await?;
        Ok(())
    }
}
