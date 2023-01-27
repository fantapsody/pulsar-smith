use std::sync::{Arc};
use actix_web::{get, patch, App, HttpResponse, HttpServer, web};
use actix_web::dev::Server;
use actix_web::web::Data;
use async_trait::async_trait;
use clap::Parser;
use prometheus_client::encoding::text::encode;
use tokio::sync::RwLock;

use crate::cmd::cmd::AsyncCmd;
use crate::cmd::commons::ProducerOpts;
use crate::context::PulsarContext;
use crate::error::Error;
use crate::perf::{DynamicConfigPatch, PerfServer};

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
    num_clients: u32,

    #[arg(long, default_value = "1")]
    num_producers_per_client: u32,

    #[arg(long, default_value = "1000")]
    message_size: usize,
}

#[async_trait]
impl AsyncCmd for PerfProduceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let opts = crate::perf::PerfOpts {
            pulsar_config: pulsar_ctx.get_config().clone(),
            producer_opts: self.producer_opts.clone(),
            rate: self.rate,
            parallelism: self.parallelism,
            num_clients: self.num_clients,
            num_producers_per_client: self.num_producers_per_client,
            message_size: self.message_size,
        };
        let mut perf_server = PerfServer::new(opts);
        perf_server.start().await?;
        let perf_server = Arc::new(RwLock::new(perf_server));
        let http_server = Self::run_http_server(perf_server.clone()).await?;
        let http_server_handle = http_server.handle();

        let http_server_job = tokio::spawn(async move {
            http_server.await.unwrap();
            info!("HTTP server stopped");
            ()
        });

        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to monitor the interrupt signal {}", e);
        }
        info!("Received interrupt signal");
        http_server_handle.stop(true).await;
        let mut write_guard = perf_server.write().await;
        if let Err(e) = write_guard.stop().await {
            error!("Failed to stop perf server {}", e);
        }
        http_server_job.await.unwrap();

        Ok(())
    }
}

impl PerfProduceOpts {
    async fn run_http_server(perf_server: Arc<RwLock<PerfServer>>) -> Result<Server, Box<dyn std::error::Error>> {
        let server = HttpServer::new(move ||
            {
                App::new()
                    .app_data(Data::from(perf_server.clone()))
                    .service(metrics)
                    .service(get_config)
                    .service(patch_config)
            })
            .disable_signals()
            .bind(("127.0.0.1", 8001))?
            .run();
        Ok(server)
    }
}

#[get("/metrics")]
async fn metrics(perf_server: Data<RwLock<PerfServer>>) -> HttpResponse {
    let mut buf = String::new();
    let read_guard = perf_server.read().await;
    let registry = read_guard.get_registry().await;
    let registry_guard = registry.lock().await;
    encode(&mut buf, &registry_guard).unwrap();
    HttpResponse::Ok()
        .body(buf)
}

#[get("/config")]
async fn get_config(perf_server: Data<RwLock<PerfServer>>) -> HttpResponse {
    let read_guard = perf_server.read().await;
    let config = read_guard.get_config().await;
    HttpResponse::Ok()
        .json(&config)
}

#[patch("/config")]
async fn patch_config(config_patch: web::Json<DynamicConfigPatch>,
                      perf_server: Data<RwLock<PerfServer>>) -> HttpResponse {
    let read_guard = perf_server.read().await;
    let config = read_guard.update_config(config_patch.0).await.unwrap();
    HttpResponse::Ok()
        .json(&config)
}

