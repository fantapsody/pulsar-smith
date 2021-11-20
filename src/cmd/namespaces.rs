use std::error::Error;

use clap::Clap;

use crate::context::PulsarContext;
use crate::admin::namespaces::NamespacePolicies;

#[derive(Clap, Debug, Clone)]
pub struct NamespacesOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl NamespacesOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        match &self.cmd {
            Command::List(opts) => opts.run(pulsar_ctx).await?,
            Command::Create(opts) => opts.run(pulsar_ctx).await?,
            Command::Permissions(opts) => opts.run(pulsar_ctx).await?,
            Command::GrantPermission(opts) => opts.run(pulsar_ctx).await?,
            Command::RevokePermission(opts) => opts.run(pulsar_ctx).await?,
        }
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    Create(CreateOpts),
    Permissions(PermissionsOpts),
    GrantPermission(GrantPermissionOpts),
    RevokePermission(RevokePermissionOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct ListOpts {
    tenant: String,
}

impl ListOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
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
    tenant: String,

    #[clap(short = 'b', long)]
    bundles: Option<u64>,

    #[clap(short = 'c', long)]
    clusters: Vec<String>,
}

impl From<&CreateOpts> for NamespacePolicies {
    fn from(opts: &CreateOpts) -> Self {
        NamespacePolicies{
            bundles: opts.bundles.clone(),
            clusters: None,
        }
    }
}

impl CreateOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .create(self.tenant.as_str(), &self.into())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct PermissionsOpts {
    namespace: String,
}

impl PermissionsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
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
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
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
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .namespaces()
            .revoke_permission(self.namespace.as_str(), self.role.as_str())
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}
