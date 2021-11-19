use std::error::Error;

use clap::Clap;

use crate::context::PulsarContext;
use crate::admin::tenants::TenantInfo;

#[derive(Clap, Debug, Clone)]
pub struct TenantsOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl TenantsOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        match &self.cmd {
            Command::List(opts) => opts.run(pulsar_ctx).await?,
            Command::Create(opts) => opts.run(pulsar_ctx).await?,
            Command::Get(opts) => opts.run(pulsar_ctx).await?,
        }
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

#[derive(Clap, Debug, Clone)]
pub struct CreateOpts {
    pub tenant: String,

    #[clap(short = 'r', long)]
    pub admin_roles: Option<Vec<String>>,

    #[clap(short = 'c', long)]
    pub allowed_clusters: Option<Vec<String>>,
}

impl CreateOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .tenants()
            .create(self.tenant.as_str(), TenantInfo{
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

impl GetOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let r = pulsar_ctx.admin().await?
            .tenants()
            .get(self.tenant.as_str())
            .await?;
        println!("{}", serde_json::to_string(&r)?);
        Ok(())
    }
}
