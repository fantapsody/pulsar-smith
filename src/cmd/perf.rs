use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use pulsar::producer::SendFuture;
use rand::Rng;
use tokio::sync::oneshot::Sender;
use tokio::time::Instant;

use crate::cmd::cmd::AsyncCmd;
use crate::cmd::commons::ProducerOpts;
use crate::context::PulsarContext;
use crate::error::Error;

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

    #[arg(long)]
    rate: Option<i32>,

    #[arg(short = 'p', long, default_value = "1")]
    parallelism: i32,

    #[arg(long, default_value = "1")]
    num_producers: i32,

    #[arg(long, default_value = "1000")]
    message_size: usize,
}

#[async_trait]
impl AsyncCmd for PerfProduceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let client = pulsar_ctx.client().await?;
        let (content_sender, content_receiver) = async_channel::bounded::<Vec<u8>>(10);
        let (msg_sender, msg_receiver) = async_channel::bounded::<(Vec<u8>, Sender<SendFuture>)>(100);
        let (tick_sender, _tick_receiver) = tokio::sync::broadcast::channel::<Instant>(1);

        for i in 0..self.parallelism {
            let content_rx = content_receiver.clone();
            let msg_tx = msg_sender.clone();
            tokio::spawn(async move {
                info!("Started sender {}", i);
                while let Ok(content) = content_rx.recv().await {
                    let (receipt_tx, receipt_rx) = tokio::sync::oneshot::channel();
                    msg_tx.send((content, receipt_tx)).await.unwrap();
                    receipt_rx.await.unwrap()
                        .await.unwrap();
                }
            });
        }

        for i in 0..self.num_producers {
            let mut tick_rx = tick_sender.subscribe();
            let msg_rx = msg_receiver.clone();
            let mut builder = self.producer_opts.producer_builder(client)?;
            if let Some(name) = &self.producer_opts.name {
                builder = builder.with_name(format!("{}-{}", name, i));
            }
            let mut producer = builder
                .build()
                .await?;
            tokio::spawn(async move {
                info!("Started producer {}", i);
                loop {
                    tokio::select! {
                        msg = msg_rx.recv() => {
                            let (content, receipt_tx) = msg.unwrap();
                            let produce_future = producer.create_message()
                            .with_content(content)
                            .send()
                            .await.unwrap();
                            if let Err(_) = receipt_tx.send(produce_future) {
                                error!("failed to send receipt");
                            }
                        }
                        _ = tick_rx.recv() => {
                            trace!("Got tick to flush on producer [{}]", i);
                            producer.send_batch().await.unwrap();
                        }
                    }
                }
            });
        }

        let batching_max_publish_latency_ms = self.producer_opts.batching_max_publish_latency_ms;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(batching_max_publish_latency_ms)).await;
                tick_sender.send(Instant::now()).unwrap();
            }
        });

        loop {
            content_sender.send(self.generate_content()).await.unwrap();
        }
    }
}

impl PerfProduceOpts {
    fn generate_content(&self) -> Vec<u8> {
        let mut vec = Vec::with_capacity(self.message_size);
        for _ in 0..self.message_size {
            vec.push('a' as u8 + rand::thread_rng().gen_range(0..26));
        }
        vec
    }
}
