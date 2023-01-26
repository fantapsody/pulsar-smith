use std::error::Error;
use std::sync::Arc;
use async_channel::Receiver;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use pulsar::{Pulsar, TokioExecutor};
use tokio::sync::{Mutex, RwLock};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use crate::cmd::commons::ProducerOpts;
use crate::context::PulsarContext;
use crate::perf::producer::PerfProducer;
use crate::perf::server::{DynamicConfig, Message, MessageReceipt, PerfOpts};

pub(crate) struct PerfClient {
    state: Arc<Mutex<PerfClientState>>,
}

struct PerfClientState {
    client_id: u32,
    pulsar_client: Arc<Pulsar<TokioExecutor>>,
    config: PerfClientDynamicConfig,
    producer_opts: Arc<ProducerOpts>,
    tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
    producer_sent_counter_family: Family<Vec<(String, String)>, Counter>,
    job: Option<JoinHandle<()>>,
    producers: Vec<PerfProducer>,
}

#[derive(Clone, Debug)]
pub(crate) struct PerfClientDynamicConfig {
    pub(crate) num_producers: u32,
}

impl PerfClient {
    pub(crate) fn new(client_id: u32,
                      config: PerfClientDynamicConfig,
                      pulsar_client: Pulsar<TokioExecutor>,
                      producer_opts: Arc<ProducerOpts>,
                      tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
                      producer_sent_counter_family: Family<Vec<(String, String)>, Counter>) -> Self {
        Self {
            state: Arc::new(Mutex::new(PerfClientState {
                client_id,
                pulsar_client: Arc::new(pulsar_client),
                config,
                producer_opts,
                tick_receiver,
                producer_sent_counter_family,
                job: None,
                producers: vec![],
            }))
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let state = self.state.clone();
        let mut guard = self.state.lock().await;
        let client_id = guard.client_id;
        if guard.job.is_some() {
            return Err(format!("Perf client {} already started", guard.client_id).into());
        }
        guard.job = Some(tokio::spawn(async move {
            if let Err(e) = Self::control_loop(state).await {
                error!("Failed to run perf client {}: {}", client_id, e);
            }
            ()
        }));
        drop(guard);
        self.reconcile().await?;
        Ok(())
    }

    pub(crate) async fn update_config(&mut self, config: PerfClientDynamicConfig) {
        let mut guard = self.state.lock().await;
        guard.config = config;
    }

    pub(crate) async fn reconcile(&mut self) -> Result<(), Box<dyn Error>> {
        let mut guard = self.state.lock().await;
        let client_id = guard.client_id;
        let config = guard.config.clone();
        let pulsar_client = guard.pulsar_client.clone();
        let producer_opts = guard.producer_opts.clone();
        let tick_receiver = guard.tick_receiver.clone();
        let producer_sent_counter_family = guard.producer_sent_counter_family.clone();

        let mut producers = &mut guard.producers;
        while producers.len() < config.num_producers as usize {
            let producer_name = format!("perf-{}-{}", client_id, producers.len());
            let producer = producer_opts.producer_builder(&pulsar_client)?
                .with_name(producer_name.clone())
                .build().await?;
            let mut producer = PerfProducer::new(producer_name,
                                                 producer,
                                                 tick_receiver.clone(),
                                                 producer_sent_counter_family.clone());
            producer.start().await?;
            producers.push(producer);
        }
        while producers.len() > config.num_producers as usize {
            let mut producer = producers.pop().unwrap();
            producer.stop().await?;
        }

        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    async fn control_loop(state: Arc<Mutex<PerfClientState>>) -> Result<(), Box<dyn Error>> {
        let guard = state.lock().await;
        info!("Started perf client {}", guard.client_id);

        Ok(())
    }
}
