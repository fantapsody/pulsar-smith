use std::borrow::BorrowMut;
use std::error::Error;
use std::fmt::format;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use actix_web::{get, patch, App, HttpServer, Responder, web, HttpResponse};
use actix_web::web::{Data, resource};
use async_channel::Receiver;
use futures::future::err;
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
use serde::{Serialize, Deserialize};
use crate::cmd::commons::ProducerOpts;
use crate::config::PulsarConfig;
use crate::context::PulsarContext;
use crate::perf::client::{PerfClient, PerfClientDynamicConfig};
use crate::perf::ticker::Ticker;

#[derive(Clone)]
pub struct PerfOpts {
    pub pulsar_config: PulsarConfig,

    pub producer_opts: ProducerOpts,

    pub rate: Option<u32>,

    pub parallelism: i32,

    pub num_clients: u32,

    pub num_producers_per_client: u32,

    pub message_size: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DynamicConfig {
    rate: u32,

    num_clients: u32,

    num_producers_per_clients: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct DynamicConfigPatch {
    rate: Option<u32>,

    num_clients: Option<u32>,

    num_producers_per_clients: Option<u32>,
}

pub struct PerfServer {
    opts: PerfOpts,
    perf_job: Option<JoinHandle<()>>,
    state: Arc<Mutex<PerfServerState>>,
}

pub struct PerfServerState {
    opts: PerfOpts,
    config: DynamicConfig,
    registry: Arc<Mutex<Registry>>,
    request_sender: async_channel::Sender<(ControlRequest, Sender<ControlResponse>)>,
    request_receiver: Receiver<(ControlRequest, Sender<ControlResponse>)>,
    tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
    ticker: Ticker,
    clients: Vec<PerfClient>,
}

impl PerfServer {
    pub fn new(opts: PerfOpts) -> Self {
        let (request_sender, request_receiver) = async_channel::bounded::<(ControlRequest, Sender<ControlResponse>)>(100);
        let (tick_sender, tick_receiver) = async_channel::bounded::<(Message, Sender<MessageReceipt>)>(100);
        let registry = Arc::new(Mutex::new(Registry::default()));
        let config = DynamicConfig {
            rate: opts.rate.unwrap_or(0),
            num_clients: opts.num_clients,
            num_producers_per_clients: opts.num_producers_per_client,
        };
        Self {
            perf_job: None,
            state: Arc::new(Mutex::new(PerfServerState {
                opts: opts.clone(),
                request_sender,
                request_receiver,
                tick_receiver,
                registry: registry.clone(),
                ticker: Ticker::new(tick_sender, registry.clone()),
                config,
                clients: vec![],
            })),
            opts,
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

#[derive(Debug)]
pub struct Message {}

#[derive(Debug)]
pub struct MessageReceipt {}

#[derive(Debug)]
enum ControlRequest {
    UpdateConfig(DynamicConfigPatch),
}

#[derive(Debug)]
enum ControlResponse {}

impl PerfServer {
    async fn start_perf(&mut self) -> Result<(), Box<dyn Error>> {
        let opts = self.opts.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::control_loop(state).await {
                error!("Failed to run the control loop: {}", e)
            }
        });
        // let state = self.state.clone();
        // self.perf_job = Some(tokio::spawn(async move {
        //     if let Err(err) = Self::run_perf(opts, state).await {
        //         error!("Failed to run perf {}", err);
        //     }
        // }));
        Ok(())
    }

    async fn control_loop(state: Arc<Mutex<PerfServerState>>) -> Result<(), Box<dyn Error>> {
        info!("Perf server controller started");
        let mut guard = state.lock().await;
        let receiver = guard.request_receiver.clone();
        drop(guard);

        loop {
            match Self::reconcile_state(state.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to reconcile perf state: {}", e)
                }
            };
            match receiver.recv().await {
                Ok((request, resp_sender)) => {
                    match request {
                        ControlRequest::UpdateConfig(new_config) => {
                            let mut guard = state.lock().await;
                            if let Some(rate) = new_config.rate {
                                guard.config.rate = rate;
                            }
                            if let Some(num_clients) = new_config.num_clients {
                                guard.config.num_clients = num_clients;
                            }
                            if let Some(num_producers_per_clients) = new_config.num_producers_per_clients {
                                guard.config.num_producers_per_clients = num_producers_per_clients;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to receive control request [{}]", e);
                    break;
                }
            }
        }
        info!("Perf server controller ended");
        Ok(())
    }

    async fn reconcile_state(state: Arc<Mutex<PerfServerState>>) -> Result<(), Box<dyn Error>> {
        let mut guard = state.lock().await;
        let producer_opts = Arc::new(guard.opts.producer_opts.clone());
        let ctx = PulsarContext::from(guard.opts.pulsar_config.clone());
        let config = guard.config.clone();
        let tick_receiver = guard.tick_receiver.clone();
        let registry = guard.registry.clone();

        let producer_sent_counter_family = Family::<Vec<(String, String)>, Counter>::default();

        {
            let mut registry_guard = registry.lock().await;
            registry_guard.borrow_mut().register("producer_sent_counter_family", "Producer sent message counters", producer_sent_counter_family.clone());
        }

        guard.ticker.update_rate(config.rate);
        if !guard.ticker.is_started().await {
            guard.ticker.start().await?;
        }

        let client_config = PerfClientDynamicConfig {
            num_producers: config.num_producers_per_clients,
        };
        let mut clients = &mut guard.clients;
        while clients.len() > config.num_clients as usize {
            let mut client = clients.pop().unwrap();
            client.stop().await?;
        }
        for client in clients.iter_mut() {
            client.update_config(client_config.clone()).await;
            client.reconcile().await?;
        }
        while clients.len() < config.num_clients as usize {
            let pulsar_client = ctx.new_client().await?;
            let mut client = PerfClient::new(clients.len() as u32,
                                             client_config.clone(),
                                             pulsar_client,
                                             producer_opts.clone(),
                                             tick_receiver.clone(),
                                             producer_sent_counter_family.clone());
            client.start().await?;
            clients.push(client);
        }

        Ok(())
    }

    async fn run_perf(opts: PerfOpts, state: Arc<Mutex<PerfServerState>>) -> Result<(), Box<dyn Error>> {
        let mut state_guard = state.lock().await;
        let msg_receiver = state_guard.tick_receiver.clone();
        let registry = state_guard.registry.clone();
        drop(state_guard);

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
        Ok(())
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
                                    client_id: u32,
                                    num_producers_per_client: u32,
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
                    msg.1.send(MessageReceipt {}).unwrap();
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

#[get("/metrics")]
async fn metrics(registry: Data<Mutex<Registry>>) -> HttpResponse {
    let mut buf = String::new();
    let registry = registry.lock().await;
    encode(&mut buf, &registry).unwrap();
    HttpResponse::Ok()
        .body(buf)
}

#[get("/config")]
async fn get_config(state: Data<Mutex<PerfServerState>>) -> HttpResponse {
    let state_guard = state.lock().await;
    HttpResponse::Ok()
        .json(&state_guard.config)
}

#[patch("/config")]
async fn patch_config(config_patch: web::Json<DynamicConfigPatch>,
                      state: Data<Mutex<PerfServerState>>) -> HttpResponse {
    let state_guard = state.lock().await;
    let (resp_sender, resp_receiver) = tokio::sync::oneshot::channel();
    state_guard.request_sender.send((ControlRequest::UpdateConfig(config_patch.0), resp_sender)).await.unwrap();
    HttpResponse::Ok()
        .json(&state_guard.config)
}

impl PerfServer {
    async fn run_http_server(&mut self) -> Result<(), Box<dyn Error>> {
        let state_guard = self.state.lock().await;
        let registry = state_guard.registry.clone();
        drop(state_guard);
        let state = self.state.clone();

        let server = HttpServer::new(move ||
            {
                App::new()
                    .app_data(Data::from(registry.clone()))
                    .app_data(Data::from(state.clone()))
                    .service(metrics)
                    .service(get_config)
                    .service(patch_config)
            })
            .bind(("127.0.0.1", 8001))?
            .run();
        server.await.unwrap();
        Ok(())
    }
}
