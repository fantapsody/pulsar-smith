use std::error::Error;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use async_channel::Sender;
use governor::{clock, Quota, RateLimiter};
use prometheus_client::metrics::counter::{Atomic, Counter};
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use std::borrow::BorrowMut;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use crate::perf::server::{Message, MessageReceipt};

pub(crate) struct Ticker {
    sender: Sender<(Message, tokio::sync::oneshot::Sender<MessageReceipt>)>,
    registry: Arc<Mutex<Registry>>,
    mutex: Mutex<()>,
    rate: Arc<AtomicU32>,
    is_running: Arc<AtomicBool>,
    job: Option<JoinHandle<()>>,
}

impl Ticker {
    pub(crate) fn new(sender: Sender<(Message, tokio::sync::oneshot::Sender<MessageReceipt>)>,
                      registry: Arc<Mutex<Registry>>) -> Self {
        Self {
            mutex: Default::default(),
            rate: Arc::new(AtomicU32::new(0)),
            sender,
            registry,
            is_running: Arc::new(AtomicBool::new(false)),
            job: None,
        }
    }

    pub async fn is_started(&self) -> bool {
        let _guard = self.mutex.lock().await;
        self.job.is_some()
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        let _guard = self.mutex.lock().await;
        if self.job.is_some() {
            return Err(format!("The ticker is running").into());
        }
        let rate = self.rate.clone();
        let sender = self.sender.clone();
        let registry = self.registry.clone();
        let is_running = self.is_running.clone();
        is_running.store(true, Ordering::Release);
        self.job = Some(tokio::spawn(async move {
            Self::send_ticks(is_running, sender, rate.clone(), registry).await;
            ()
        }));
        Ok(())
    }

    pub fn update_rate(&mut self, new_rate: u32) {
        self.rate.store(new_rate, Ordering::Release);
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let _guard = self.mutex.lock().await;
        if let Some(job) = self.job.take() {
            self.is_running.store(false, Ordering::Release);
            job.await?;
        }
        info!("Stopped ticker");
        Ok(())
    }

    async fn send_ticks(is_running: Arc<AtomicBool>,
                        sender: Sender<(Message, tokio::sync::oneshot::Sender<MessageReceipt>)>,
                        rate: Arc<AtomicU32>,
                        registry: Arc<Mutex<Registry>>) {
        let msg_issued_counter: Counter = Counter::default();
        let msg_sent_counter: Counter = Counter::default();
        {
            let mut registry_guard = registry.lock().await;
            registry_guard.borrow_mut().register("msg_issued", "Message issued", msg_issued_counter.clone());
            registry_guard.borrow_mut().register("msg_sent", "Message sent", msg_sent_counter.clone());
        }
        let mut rate_limiter: Option<RateLimiter<NotKeyed, InMemoryState, clock::DefaultClock, NoOpMiddleware>> = None;
        let mut current_rate = 0;
        info!("Send tick loop started");
        while is_running.load(Ordering::Acquire) {
            let r = rate.get();
            if r != current_rate {
                if r > 0 {
                    rate_limiter = Some(RateLimiter::direct(Quota::per_second(NonZeroU32::new(r).unwrap())));
                    info!("Set tick rate limiter to {}", r);
                } else {
                    rate_limiter = None;
                    info!("Disable tick rate limiter");
                }
                current_rate = r;
            }
            if let Some(limiter) = &rate_limiter {
                limiter.until_ready().await;
            }

            let (receipt_tx, receipt_rx) = tokio::sync::oneshot::channel();
            msg_issued_counter.inc();

            if let Err(e) = sender.send((Message {}, receipt_tx)).await {
                error!("Failed to send tick: {}", e);
                break;
            }
            let msg_sent_counter = msg_sent_counter.clone();
            let is_running = is_running.clone();
            tokio::spawn(async move {
                match receipt_rx.await {
                    Ok(r) => {
                        msg_sent_counter.inc();
                        trace!("received send receipt: {:?}", r)
                    }
                    Err(e) => {
                        if is_running.load(Ordering::Acquire) {
                            error!("receive send receipt error: {}", e)
                        }
                    }
                };
            });
        }
        info!("Send tick loop ended");
    }
}

