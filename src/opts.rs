use clap::Parser;

use crate::cmd::auth::AuthOpts;
use crate::cmd::clusters::ClustersOpts;
use crate::cmd::consume::ConsumeOpts;
use crate::cmd::functions::FunctionOpts;
use crate::cmd::namespaces::NamespacesOpts;
use crate::cmd::produce::ProduceOpts;
use crate::cmd::sinks::SinksOpts;
use crate::cmd::tenants::TenantsOpts;
use crate::cmd::topics::TopicsOpts;
use crate::config::PulsarConfig;
use crate::cmd::perf::PerfOpts;

#[derive(Parser, Debug, Clone)]
pub struct PulsarOpts {
    #[arg(long)]
    pub context: Option<String>,

    #[arg(long)]
    pub url: Option<String>,

    #[arg(long)]
    pub admin_url: Option<String>,

    #[arg(long)]
    pub proxy_url: Option<String>,

    #[arg(long)]
    pub auth_name: Option<String>,

    #[arg(long)]
    pub auth_params: Option<String>,

    #[arg(long)]
    pub allow_insecure_connection: Option<bool>,

    #[arg(long)]
    pub tls_hostname_verification_enabled: Option<bool>,

    #[command(subcommand)]
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
            tls_hostname_verification_enabled: self.tls_hostname_verification_enabled.unwrap_or(true),
        }
    }
}

#[derive(Parser, Debug, Clone)]
pub enum Command {
    Produce(ProduceOpts),
    Consume(ConsumeOpts),
    Clusters(ClustersOpts),
    Tenants(TenantsOpts),
    Namespaces(NamespacesOpts),
    Topics(TopicsOpts),
    Auth(AuthOpts),
    Functions(FunctionOpts),
    Sinks(SinksOpts),
    Perf(PerfOpts),
}

pub fn parse_opts() -> PulsarOpts {
    PulsarOpts::parse()
}
