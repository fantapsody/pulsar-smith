use std::borrow::BorrowMut;
use std::error::Error;
use std::sync::Arc;
use async_channel::Receiver;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use pulsar::{Producer, TokioExecutor};
use tokio::sync::Mutex;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use crate::perf::server::{Message, MessageReceipt};

pub(crate) struct PerfProducer {
    state: Arc<Mutex<PerfProducerState>>,
}

struct PerfProducerState {
    name: String,
    producer: Arc<Mutex<Producer<TokioExecutor>>>,
    tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
    producer_sent_counter_family: Family<Vec<(String, String)>, Counter>,
    job: Option<JoinHandle<()>>,
}

impl PerfProducer {
    pub(crate) fn new(name: String,
                      producer: Producer<TokioExecutor>,
                      tick_receiver: Receiver<(Message, Sender<MessageReceipt>)>,
                      producer_sent_counter_family: Family<Vec<(String, String)>, Counter>) -> Self {
        Self {
            state: Arc::new(Mutex::new(PerfProducerState {
                name,
                producer: Arc::new(Mutex::new(producer)),
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
        info!("Started perf producer {}", name);
        guard.job = Some(tokio::spawn(async move {
            if let Err(e) = Self::run_perf(state).await {
                error!("Failed to run producer perf {}", e);
            }
        }));

        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let mut guard = self.state.lock().await;
        let name = guard.name.clone();
        info!("Stopped perf producer {}", name);
        Ok(())
    }

    async fn run_perf(state: Arc<Mutex<PerfProducerState>>) -> Result<(), Box<dyn Error>> {
        let guard = state.lock().await;
        let name = guard.name.clone();
        let mut producer = guard.producer.clone();
        let tick_receiver = guard.tick_receiver.clone();
        let producer_sent_counter_family = guard.producer_sent_counter_family.clone();
        drop(guard);
        let mut producer_guard = producer.lock().await;
        let mut producer = producer_guard.borrow_mut();

        info!("Producer perf [{}] started", name);
        loop {
            match tick_receiver.recv().await {
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

    fn generate_content(message_size: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(message_size);
        for _ in 0..message_size {
            vec.push('a' as u8);// + rand::thread_rng().gen_range(0..26));
        }
        vec
    }
}
