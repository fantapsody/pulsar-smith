use clap::Clap;
use crate::context::PulsarContext;
use crate::cmd::produce::ProduceOpts;
use crate::cmd::consume::ConsumeOpts;

#[derive(Clap, Debug, Clone)]
#[clap(version = "1.0", author = "Yang Yang <yyang@streamnative.io>")]
pub struct PulsarOpts {
    #[clap(long, default_value = "pulsar://localhost:6650")]
    pub url: String,

    #[clap(long, default_value = "http://localhost:8080")]
    pub admin_service_url: String,

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
    pub fn create_context(&self) -> PulsarContext {
        PulsarContext::create(self.clone())
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
