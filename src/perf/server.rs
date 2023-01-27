use std::borrow::BorrowMut;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_channel::Receiver;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use tokio::sync::{Mutex, RwLock};
use tokio::sync::oneshot::Sender;
use serde::{Serialize, Deserialize};
use tokio::task::JoinHandle;
use crate::cmd::commons::ProducerOpts;
use crate::config::PulsarConfig;
use crate::context::PulsarContext;
use crate::perf::client::{PerfClient};
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
    pub(crate) rate: u32,

    pub(crate) num_clients: u32,

    pub(crate) num_producers_per_client: u32,

    pub(crate) message_size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DynamicConfigPatch {
    rate: Option<u32>,

    num_clients: Option<u32>,

    num_producers_per_client: Option<u32>,

    message_size: Option<usize>,
}

pub struct PerfServer {
    mutex: Mutex<()>,
    ctrl_loop_handle: Option<JoinHandle<()>>,
    is_running: Arc<AtomicBool>,
    request_sender: async_channel::Sender<ControlRequest>,
    state: Arc<Mutex<PerfServerState>>,
}

struct PerfServerState {
    opts: PerfOpts,
    config: Arc<RwLock<DynamicConfig>>,
    registry: Arc<Mutex<Registry>>,
    request_receiver: Receiver<ControlRequest>,
    tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
    ticker: Ticker,
    clients: Vec<PerfClient>,
}

impl PerfServer {
    pub fn new(opts: PerfOpts) -> Self {
        let (request_sender, request_receiver) = async_channel::bounded::<ControlRequest>(5);
        let (tick_sender, tick_receiver) = async_channel::bounded::<(Message, Sender<MessageReceipt>)>(100);
        let registry = Arc::new(Mutex::new(Registry::default()));
        let config = DynamicConfig {
            rate: opts.rate.unwrap_or(0),
            num_clients: opts.num_clients,
            num_producers_per_client: opts.num_producers_per_client,
            message_size: opts.message_size,
        };
        Self {
            mutex: Mutex::new(()),
            ctrl_loop_handle: None,
            is_running: Arc::new(AtomicBool::new(false)),
            request_sender,
            state: Arc::new(Mutex::new(PerfServerState {
                opts: opts.clone(),
                request_receiver,
                tick_receiver,
                registry: registry.clone(),
                ticker: Ticker::new(tick_sender, registry.clone()),
                config: Arc::new(RwLock::new(config)),
                clients: vec![],
            })),

        }
    }
}

impl PerfServer {
    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let _lock = self.mutex.lock().await;
        let state = self.state.clone();
        let is_running = self.is_running.clone();
        is_running.store(true, Ordering::Release);
        self.ctrl_loop_handle = Some(tokio::spawn(async move {
            if let Err(e) = Self::control_loop(is_running, state).await {
                error!("Failed to run the control loop: {}", e)
            }
        }));
        info!("Started perf server");
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let _lock = self.mutex.lock().await;
        if let Some(handle) = self.ctrl_loop_handle.take() {
            self.is_running.store(false, Ordering::Release);
            self.request_sender.send(ControlRequest::Stop).await?;
            handle.await?;
        }
        info!("Stopped perf server");
        Ok(())
    }

    pub async fn get_registry(&self) -> Arc<Mutex<Registry>> {
        let guard = self.state.lock().await;
        guard.registry.clone()
    }

    pub async fn get_config(&self) -> DynamicConfig {
        let guard = self.state.lock().await;
        let config = guard.config.read().await;
        config.clone()
    }

    pub async fn update_config(&self, config: DynamicConfigPatch) -> DynamicConfig {
        let (resp_sender, resp_receiver) = tokio::sync::oneshot::channel();
        self.request_sender.send(ControlRequest::UpdateConfig(config, resp_sender)).await.unwrap();
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
    UpdateConfig(DynamicConfigPatch, Sender<ControlResponse>),
    Stop,
}

#[derive(Debug)]
enum ControlResponse {
    UpdateConfig(DynamicConfig)
}

impl PerfServer {
    async fn control_loop(is_running: Arc<AtomicBool>,
                          state: Arc<Mutex<PerfServerState>>) -> Result<(), Box<dyn Error>> {
        info!("Perf server controller started");
        let guard = state.lock().await;
        let receiver = guard.request_receiver.clone();
        drop(guard);

        while is_running.load(Ordering::Acquire) {
            match Self::reconcile_state(is_running.clone(), state.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to reconcile perf state: {}", e)
                }
            };
            match receiver.recv().await {
                Ok(request) => {
                    match request {
                        ControlRequest::UpdateConfig(new_config, resp_sender) => {
                            let guard = state.lock().await;
                            let mut config_guard = guard.config.write().await;
                            if let Some(rate) = new_config.rate {
                                info!("Updated rate config: {} -> {}", config_guard.rate, rate);
                                config_guard.rate = rate;
                            }
                            if let Some(num_clients) = new_config.num_clients {
                                info!("Updated num_clients config: {} -> {}", config_guard.num_clients, num_clients);
                                config_guard.num_clients = num_clients;
                            }
                            if let Some(num_producers_per_client) = new_config.num_producers_per_client {
                                info!("Updated num_producers_per_clients config: {} -> {}", config_guard.num_producers_per_client, num_producers_per_client);
                                config_guard.num_producers_per_client = num_producers_per_client;
                            }
                            if let Some(message_size) = new_config.message_size {
                                info!("Updated message_size config: {} -> {}", config_guard.message_size, message_size);
                                config_guard.message_size = message_size;
                            }
                            if let Err(_) = resp_sender.send(ControlResponse::UpdateConfig(config_guard.clone())) {
                                warn!("Failed to send control response");
                            }
                        }
                        ControlRequest::Stop => {
                            info!("Received stop command");
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to receive control request [{}]", e);
                    break;
                }
            }
        }
        Self::reconcile_state(is_running.clone(), state.clone()).await?;
        info!("Perf server controller ended");
        Ok(())
    }

    async fn reconcile_state(is_running: Arc<AtomicBool>, state: Arc<Mutex<PerfServerState>>) -> Result<(), Box<dyn Error>> {
        let mut guard = state.lock().await;
        let producer_opts = Arc::new(guard.opts.producer_opts.clone());
        let ctx = PulsarContext::from(guard.opts.pulsar_config.clone());
        let config = guard.config.clone();
        let tick_receiver = guard.tick_receiver.clone();
        let registry = guard.registry.clone();

        let (rate, num_clients) = {
            let config = config.read().await;
            (config.rate, config.num_clients)
        };

        let producer_sent_counter_family = Family::<Vec<(String, String)>, Counter>::default();

        {
            let mut registry_guard = registry.lock().await;
            registry_guard.borrow_mut().register("producer_sent_counter_family", "Producer sent message counters", producer_sent_counter_family.clone());
        }

        if is_running.load(Ordering::Acquire) {
            guard.ticker.update_rate(rate);
            if !guard.ticker.is_started().await {
                guard.ticker.start().await?;
            }
        } else {
            guard.ticker.stop().await?;
        }

        let desired_num_clients = if is_running.load(Ordering::Acquire) {
            num_clients as usize
        } else {
            0
        };
        let clients = &mut guard.clients;
        while clients.len() > desired_num_clients {
            let mut client = clients.pop().unwrap();
            client.stop().await?;
        }
        for client in clients.iter_mut() {
            client.reconcile().await?;
        }
        while clients.len() < desired_num_clients {
            let pulsar_client = ctx.new_client().await?;
            let mut client = PerfClient::new(clients.len() as u32,
                                             config.clone(),
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
