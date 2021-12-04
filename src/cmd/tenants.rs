use async_trait::async_trait;
use clap::Clap;

use crate::admin::tenants::TenantInfo;
use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct TenantsOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for TenantsOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd: &dyn AsyncCmd = match &self.cmd {
            Command::List(opts) => opts,
            Command::Create(opts) => opts,
            Command::Get(opts) => opts,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    List(ListOpts),
    Create(CreateOpts),
    Get(GetOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct ListOpts {}

#[async_trait]
impl AsyncCmd for ListOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .tenants()
            .list()
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub struct CreateOpts {
    pub tenant: String,

    #[clap(short = 'r', long)]
    pub admin_roles: Option<Vec<String>>,

    #[clap(short = 'c', long)]
    pub allowed_clusters: Option<Vec<String>>,
}

#[async_trait]
impl AsyncCmd for CreateOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .tenants()
            .create(self.tenant.as_str(), TenantInfo {
                admin_roles: self.admin_roles.clone().unwrap_or(Vec::new()),
                allowed_clusters: self.allowed_clusters.clone().unwrap_or(Vec::new()),
            })
            .await?;
        println!("{:?}", r);
        Ok(())
    }
}


#[derive(Clap, Debug, Clone)]
pub struct GetOpts {
    pub tenant: String,
}

#[async_trait]
impl AsyncCmd for GetOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let r = pulsar_ctx.admin().await?
            .tenants()
            .get(self.tenant.as_str())
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}
