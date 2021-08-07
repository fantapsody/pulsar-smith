use clap::Clap;
use crate::cmd::produce::ProduceOpts;
use crate::cmd::consume::ConsumeOpts;
use crate::config::PulsarConfig;
use crate::cmd::topics::TopicsOpts;
use crate::cmd::tenants::TenantsOpts;

#[derive(Clap, Debug, Clone)]
#[clap(version = "1.0", author = "Yang Yang <yyang@streamnative.io>")]
pub struct PulsarOpts {
    #[clap(long)]
    pub context: Option<String>,

    #[clap(long)]
    pub url: Option<String>,

    #[clap(long)]
    pub admin_url: Option<String>,

    #[clap(long)]
    pub proxy_url: Option<String>,

    #[clap(long)]
    pub auth_name: Option<String>,

    #[clap(long)]
    pub auth_params: Option<String>,

    #[clap(long)]
    pub allow_insecure_connection: Option<bool>,

    #[clap(subcommand)]
    pub cmd: Command,
}

impl PulsarOpts {
    pub fn to_pulsar_config(&self) -> PulsarConfig {
        PulsarConfig {
            url: self.url.as_ref().unwrap_or(&String::from("pulsar://localhost:6650")).clone(),
            admin_url: self.admin_url.as_ref().unwrap_or(&String::from("http://localhost:8080")).clone(),
            auth_name: self.auth_name.clone(),
            auth_params: self.auth_params.clone(),
            allow_insecure_connection: self.allow_insecure_connection.unwrap_or(false),
        }
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    Produce(ProduceOpts),
    Consume(ConsumeOpts),
    Tenants(TenantsOpts),
    Topics(TopicsOpts),
}

pub fn parse_opts() -> PulsarOpts {
    PulsarOpts::parse()
}
