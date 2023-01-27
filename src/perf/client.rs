use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_channel::Receiver;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use pulsar::{Pulsar, TokioExecutor};
use tokio::sync::{Mutex, RwLock};
use tokio::sync::oneshot::Sender;
use crate::cmd::commons::ProducerOpts;
use crate::perf::producer::PerfProducer;
use crate::perf::server::{DynamicConfig, Message, MessageReceipt};

pub(crate) struct PerfClient {
    is_running: Arc<AtomicBool>,
    state: Arc<Mutex<PerfClientState>>,
}

struct PerfClientState {
    client_id: u32,
    pulsar_client: Arc<Pulsar<TokioExecutor>>,
    config: Arc<RwLock<DynamicConfig>>,
    producer_opts: Arc<ProducerOpts>,
    tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
    producer_sent_counter_family: Family<Vec<(String, String)>, Counter>,
    producers: Vec<PerfProducer>,
}

impl PerfClient {
    pub(crate) fn new(client_id: u32,
                      config: Arc<RwLock<DynamicConfig>>,
                      pulsar_client: Pulsar<TokioExecutor>,
                      producer_opts: Arc<ProducerOpts>,
                      tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
                      producer_sent_counter_family: Family<Vec<(String, String)>, Counter>) -> Self {
        Self {
            is_running: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(PerfClientState {
                client_id,
                pulsar_client: Arc::new(pulsar_client),
                config,
                producer_opts,
                tick_receiver,
                producer_sent_counter_family,
                producers: vec![],
            }))
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let guard = self.state.lock().await;
        let client_id = guard.client_id;
        drop(guard);
        self.is_running.store(true, Ordering::Release);
        self.reconcile().await?;
        info!("Perf client {} started", client_id);
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        self.is_running.store(false, Ordering::Release);
        let guard = self.state.lock().await;
        let client_id = guard.client_id;
        drop(guard);
        self.reconcile().await?;
        info!("Perf client {} stopped", client_id);
        Ok(())
    }

    pub(crate) async fn reconcile(&mut self) -> Result<(), Box<dyn Error>> {
        let mut guard = self.state.lock().await;
        let client_id = guard.client_id;
        let config = guard.config.clone();
        let pulsar_client = guard.pulsar_client.clone();
        let producer_opts = guard.producer_opts.clone();
        let tick_receiver = guard.tick_receiver.clone();
        let producer_sent_counter_family = guard.producer_sent_counter_family.clone();

        let desired_num_producers = if self.is_running.load(Ordering::Acquire) {
            let config = config.read().await;
            config.num_producers_per_client
        } else {
            0
        };
        let producers = &mut guard.producers;
        while producers.len() > desired_num_producers as usize {
            let mut producer = producers.pop().unwrap();
            producer.stop().await?;
        }
        while producers.len() < desired_num_producers as usize {
            let producer_name = format!("perf-{}-{}", client_id, producers.len());
            let producer = producer_opts.producer_builder(&pulsar_client)?
                .with_name(producer_name.clone())
                .build().await?;
            let mut producer = PerfProducer::new(producer_name,
                                                 producer,
                                                 tick_receiver.clone(),
                                                 config.clone(),
                                                 producer_sent_counter_family.clone());
            producer.start().await?;
            producers.push(producer);
        }

        Ok(())
    }
}
