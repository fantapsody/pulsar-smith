use std::borrow::BorrowMut;
use std::error::Error;
use std::sync::Arc;
use async_channel::Receiver;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;
use tokio::sync::oneshot::Sender;
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
pub struct DynamicConfig {
    rate: u32,

    num_clients: u32,

    num_producers_per_client: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DynamicConfigPatch {
    rate: Option<u32>,

    num_clients: Option<u32>,

    num_producers_per_client: Option<u32>,
}

pub struct PerfServer {
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
            num_producers_per_client: opts.num_producers_per_client,
        };
        Self {
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
        }
    }
}

impl PerfServer {
    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        self.start_perf().await?;
        // self.run_http_server().await?;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    pub async fn get_registry(&self) -> Arc<Mutex<Registry>> {
        let guard = self.state.lock().await;
        guard.registry.clone()
    }

    pub async fn get_config(&self) -> DynamicConfig {
        let guard = self.state.lock().await;
        guard.config.clone()
    }

    pub async fn update_config(&self, config: DynamicConfigPatch) -> DynamicConfig {
        let state_guard = self.state.lock().await;
        let (resp_sender, resp_receiver) = tokio::sync::oneshot::channel();
        state_guard.request_sender.send((ControlRequest::UpdateConfig(config), resp_sender)).await.unwrap();
        drop(state_guard);
        let config = resp_receiver.await.unwrap();
        match config {
            ControlResponse::UpdateConfig(c) => {
                c
            }
        }
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
enum ControlResponse {
    UpdateConfig(DynamicConfig)
}

impl PerfServer {
    async fn start_perf(&mut self) -> Result<(), Box<dyn Error>> {
        let state = self.state.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::control_loop(state).await {
                error!("Failed to run the control loop: {}", e)
            }
        });
        Ok(())
    }

    async fn control_loop(state: Arc<Mutex<PerfServerState>>) -> Result<(), Box<dyn Error>> {
        info!("Perf server controller started");
        let guard = state.lock().await;
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
                            if let Some(num_producers_per_clients) = new_config.num_producers_per_client {
                                guard.config.num_producers_per_client = num_producers_per_clients;
                            }
                            if let Err(_) = resp_sender.send(ControlResponse::UpdateConfig(guard.config.clone())) {
                                warn!("Failed to send control response");
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
        let mut guard = state.lock().await;
        guard.ticker.stop().await?;
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
            num_producers: config.num_producers_per_client,
        };
        let clients = &mut guard.clients;
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
}
