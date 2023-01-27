use std::borrow::BorrowMut;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_channel::Receiver;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use pulsar::{Producer, TokioExecutor};
use tokio::sync::{Mutex, RwLock};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use crate::perf::server::{DynamicConfig, Message, MessageReceipt};

pub(crate) struct PerfProducer {
    is_running: Arc<AtomicBool>,
    state: Arc<Mutex<PerfProducerState>>,
}

struct PerfProducerState {
    name: String,
    producer: Arc<Mutex<Producer<TokioExecutor>>>,
    tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
    config: Arc<RwLock<DynamicConfig>>,
    producer_sent_counter_family: Family<Vec<(String, String)>, Counter>,
    job: Option<JoinHandle<()>>,
}

impl PerfProducer {
    pub(crate) fn new(name: String,
                      producer: Producer<TokioExecutor>,
                      tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
                      config: Arc<RwLock<DynamicConfig>>,
                      producer_sent_counter_family: Family<Vec<(String, String)>, Counter>) -> Self {
        Self {
            is_running: Arc::new(AtomicBool::new(false)),
            state: Arc::new(Mutex::new(PerfProducerState {
                name,
                producer: Arc::new(Mutex::new(producer)),
                config: config.clone(),
                tick_receiver,
                producer_sent_counter_family,
                job: None,
            }))
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let state = self.state.clone();
        let mut guard = self.state.lock().await;
        if guard.job.is_some() {
            return Err(format!("Perf producer {} already started", guard.name).into());
        }
        let name = guard.name.clone();
        let is_running = self.is_running.clone();
        is_running.store(true, Ordering::Release);
        info!("Started perf producer {}", name);
        guard.job = Some(tokio::spawn(async move {
            if let Err(e) = Self::run_perf(is_running, state).await {
                error!("Failed to run producer perf {}", e);
            }
        }));

        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut guard = self.state.lock().await;
        let name = guard.name.clone();
        self.is_running.store(false, Ordering::Release);
        if let Some(job) = guard.job.take() {
            job.await?;
        }
        info!("Stopped perf producer {}", name);
        Ok(())
    }

    async fn run_perf(is_running: Arc<AtomicBool>, state: Arc<Mutex<PerfProducerState>>) -> Result<(), Box<dyn Error>> {
        let guard = state.lock().await;
        let name = guard.name.clone();
        let producer = guard.producer.clone();
        let tick_receiver = guard.tick_receiver.clone();
        let producer_sent_counter_family = guard.producer_sent_counter_family.clone();
        let config = guard.config.clone();
        drop(guard);
        let mut producer_guard = producer.lock().await;
        let producer = producer_guard.borrow_mut();

        info!("Producer perf [{}] started", name);
        while is_running.load(Ordering::Acquire) {
            match tick_receiver.recv().await {
                Ok(msg) => {
                    let message_size = {
                        let guard = config.read().await;
                        guard.message_size
                    };
                    let content = Self::generate_content(message_size);
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

    fn generate_content(message_size: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(message_size);
        for _ in 0..message_size {
            vec.push('a' as u8);// + rand::thread_rng().gen_range(0..26));
        }
        vec
    }
}
