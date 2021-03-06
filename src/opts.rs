use clap::Clap;
use crate::cmd::produce::ProduceOpts;
use crate::cmd::consume::ConsumeOpts;
use crate::config::PulsarConfig;

#[derive(Clap, Debug, Clone)]
#[clap(version = "1.0", author = "Yang Yang <yyang@streamnative.io>")]
pub struct PulsarOpts {
    #[clap(long)]
    pub context: Option<String>,

    #[clap(long)]
    pub url: Option<String>,

    #[clap(long)]
    pub admin_service_url: Option<String>,

    #[clap(long)]
    pub proxy_url: Option<String>,

    #[clap(long)]
    pub auth_name: Option<String>,

    #[clap(long)]
    pub auth_params: Option<String>,

    #[clap(subcommand)]
    pub cmd: Command,
}

impl PulsarOpts {
    pub fn to_pulsar_config(&self) -> PulsarConfig {
        PulsarConfig {
            url: self.url.as_ref().unwrap_or(&String::from("pulsar://localhost:6650")).clone(),
            auth_name: self.auth_name.clone(),
            auth_params: self.auth_params.clone(),
        }
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    Produce(ProduceOpts),
    Consume(ConsumeOpts),
}

pub fn parse_opts() -> PulsarOpts {
    PulsarOpts::parse()
}
