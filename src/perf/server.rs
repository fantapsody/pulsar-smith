use std::borrow::BorrowMut;
use std::error::Error;
use std::fmt::format;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use actix_web::{get, App, HttpServer, Responder, web, HttpResponse};
use actix_web::web::{Data, resource};
use async_channel::Receiver;
use governor::{Quota, RateLimiter};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use pulsar::{Producer, Pulsar, TokioExecutor};
use pulsar::producer::SendFuture;
use rand::Rng;
use tokio::sync::Mutex;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use crate::cmd::commons::ProducerOpts;
use crate::config::PulsarConfig;
use crate::context::PulsarContext;

#[derive(Clone)]
pub struct PerfOpts {
    pub pulsar_config: PulsarConfig,

    pub producer_opts: ProducerOpts,

    pub rate: Option<i32>,

    pub parallelism: i32,

    pub num_clients: i32,

    pub num_producers_per_client: i32,

    pub message_size: usize,
}

pub struct PerfServer {
    opts: PerfOpts,
    registry: Data<Mutex<Registry>>,
    perf_job: Option<JoinHandle<()>>,
}

impl PerfServer {
    pub fn new(opts: PerfOpts) -> Self {
        PerfServer {
            opts,
            registry: Data::new(Mutex::new(Registry::default())),
            perf_job: None,
        }
    }
}

impl PerfServer {
    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        self.start_perf().await?;
        self.run_http_server().await?;
        Ok(())
    }
}

#[get("/metrics")]
async fn metrics(registry: Data<Mutex<Registry>>) -> HttpResponse {
    let mut buf = String::new();
    let registry = registry.lock().await;
    encode(&mut buf, &registry).unwrap();
    HttpResponse::Ok()
        .body(buf)
}

#[derive(Debug)]
struct Message {}

#[derive(Debug)]
struct MessageReceipt {}

impl PerfServer {
    async fn start_perf(&mut self) -> Result<(), Box<dyn Error>> {
        let opts = self.opts.clone();
        let registry = self.registry.clone();
        self.perf_job = Some(tokio::spawn(async move {
            if let Err(err) = Self::run_perf(opts, registry).await {
                error!("Failed to run perf {}", err);
            }
        }));
        Ok(())
    }

    async fn run_perf(opts: PerfOpts, registry: Data<Mutex<Registry>>) -> Result<(), Box<dyn Error>> {
        let (msg_sender, msg_receiver) = async_channel::bounded::<(Message, Sender<MessageReceipt>)>(100);

        let producer_sent_counter_family = Family::<Vec<(String, String)>, Counter>::default();

        {
            let mut registry_guard = registry.lock().await;
            registry_guard.borrow_mut().register("producer_sent_counter_family", "Producer sent message counters", producer_sent_counter_family.clone());
        }

        for i in 0..opts.num_clients {
            let config = opts.pulsar_config.clone();
            let mut pulsar_client = (PulsarContext::from(config)).new_client().await?;
            let producer_opts = opts.producer_opts.clone();
            let msg_receiver = msg_receiver.clone();
            let num_producers_per_client = opts.num_producers_per_client;
            let producer_sent_counter_family = producer_sent_counter_family.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::run_single_client_perf(pulsar_client,
                                                               producer_opts,
                                                               msg_receiver,
                                                               i,
                                                               num_producers_per_client,
                                                               producer_sent_counter_family).await {
                    error!("Failed to run single client perf {}", err)
                }
            });
        }
        let mut msg_issued_counter: Counter = Counter::default();
        let mut msg_sent_counter: Counter = Counter::default();
        {
            let mut registry_guard = registry.lock().await;
            registry_guard.borrow_mut().register("msg_issued", "Message issued", msg_issued_counter.clone());
            registry_guard.borrow_mut().register("msg_sent", "Message sent", msg_sent_counter.clone());
        }
        let rate_limiter = opts.rate.map(|rate| {
            RateLimiter::direct(Quota::per_second(NonZeroU32::new(rate as u32).unwrap()))
        });
        loop {
            if let Some(limiter) = &rate_limiter {
                limiter.until_ready().await;
            }
            let (receipt_tx, receipt_rx) = tokio::sync::oneshot::channel();
            msg_issued_counter.inc();

            msg_sender.send((Message {}, receipt_tx)).await?;
            let msg_sent_counter = msg_sent_counter.clone();
            tokio::spawn(async move {
                match receipt_rx.await {
                    Ok(r) => {
                        msg_sent_counter.inc();
                        trace!("received send receipt: {:?}", r)
                    }
                    Err(e) => error!("receive send receipt error: {}", e)
                };
            });
        }
    }

    fn generate_content(message_size: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(message_size);
        for _ in 0..message_size {
            vec.push('a' as u8);// + rand::thread_rng().gen_range(0..26));
        }
        vec
    }

    async fn run_single_client_perf(mut pulsar_client: Pulsar<TokioExecutor>,
                                    producer_opts: ProducerOpts,
                                    msg_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
                                    client_id: i32,
                                    num_producers_per_client: i32,
                                    producer_sent_counter_family: Family<Vec<(String, String)>, Counter>) -> Result<(), Box<dyn Error>> {
        for i in 0..num_producers_per_client {
            let producer_name = format!("pulsar-smith-perf-{}-{}", client_id, i);
            let producer = producer_opts.producer_builder(&pulsar_client)?
                .with_name(producer_name.clone())
                .build()
                .await?;
            let msg_receiver = msg_receiver.clone();
            let producer_sent_counter_family = producer_sent_counter_family.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::produce_perf(producer,
                                                     msg_receiver,
                                                     &producer_name,
                                                     producer_sent_counter_family).await {
                    error!("Failed to run producer perf {}: [{}]", producer_name, err)
                }
            });
        }
        Ok(())
    }

    async fn produce_perf(mut producer: Producer<TokioExecutor>,
                          msg_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
                          name: &str,
                          producer_sent_counter_family: Family<Vec<(String, String)>, Counter>) -> Result<(), Box<dyn Error>> {
        info!("Producer perf [{}] started", name);
        loop {
            match msg_receiver.recv().await {
                Ok(msg) => {
                    let content = Self::generate_content(1000);
                    let r = producer.create_message()
                        .with_content(content)
                        .send()
                        .await?
                        .await;
                    match r {
                        Ok(receipt) => {
                            trace!("Sent message {:?}", receipt)
                        }
                        Err(e) => {
                            error!("Failed to send message: {}", e)
                        }
                    }
                    msg.1.send(MessageReceipt{}).unwrap();
                    producer_sent_counter_family.get_or_create(&vec![("producer".to_string(), name.to_string())]).inc();
                    continue;
                }
                Err(e) => {
                    error!("Receive message error: {}", e);
                    break;
                }
            }
        }
        info!("Producer perf [{}] stopped", name);
        Ok(())
    }
}

impl PerfServer {
    async fn run_http_server(&mut self) -> Result<(), Box<dyn Error>> {
        let registry = self.registry.clone();

        let server = HttpServer::new(move ||
            {
                App::new()
                    .app_data(registry.clone())
                    .service(metrics)
            })
            .bind(("127.0.0.1", 8001))?
            .run();
        server.await.unwrap();
        Ok(())
    }
}
