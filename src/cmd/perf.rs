use std::time::Duration;
use async_channel::SendError;

use async_trait::async_trait;
use clap::Parser;
use futures::future::err;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use pulsar::producer::SendFuture;
use rand::Rng;
use tokio::sync::oneshot::Sender;
use tokio::time::Instant;
use prometheus_client::registry::Registry;

use crate::cmd::cmd::AsyncCmd;
use crate::cmd::commons::ProducerOpts;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Parser, Debug, Clone)]
pub struct PerfOpts {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Parser, Debug, Clone)]
pub enum Command {
    Produce(PerfProduceOpts),
}

#[async_trait]
impl AsyncCmd for PerfOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd = match &self.cmd {
            Command::Produce(x) => x,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Parser, Debug, Clone)]
pub struct PerfProduceOpts {
    #[command(flatten)]
    producer_opts: ProducerOpts,

    #[arg(long, default_value = "10")]
    rate: Option<u32>,

    #[arg(short = 'p', long, default_value = "1")]
    parallelism: i32,

    #[arg(long, default_value = "1")]
    num_clients: i32,

    #[arg(long, default_value = "1")]
    num_producers_per_client: i32,

    #[arg(long, default_value = "1000")]
    message_size: usize,
}

#[async_trait]
impl AsyncCmd for PerfProduceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let opts = crate::perf::PerfOpts{
            pulsar_config: pulsar_ctx.get_config().clone(),
            producer_opts: self.producer_opts.clone(),
            rate: self.rate,
            parallelism: self.parallelism,
            num_clients: self.num_clients,
            num_producers_per_client: self.num_producers_per_client,
            message_size: self.message_size,
        };
        let mut perf_server = crate::perf::PerfServer::new(opts);
        perf_server.run().await?;
        Ok(())
    }
}

