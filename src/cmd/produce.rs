use std::io::stdin;

use async_trait::async_trait;
use clap::Parser;

use crate::cmd::cmd::AsyncCmd;
use crate::cmd::commons::ProducerOpts;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Parser, Debug, Clone)]
pub struct ProduceOpts {
    #[command(flatten)]
    pub producer_opts: ProducerOpts,

    #[arg(short = 'm', long)]
    pub message: Option<String>,

    #[arg(long)]
    pub event_time: Option<u64>,
}

#[async_trait]
impl AsyncCmd for ProduceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let mut producer = self.producer_opts.producer_builder(pulsar_ctx.client().await?)?
            .build()
            .await?;

        if let Some(msg) = &self.message {
            let mut builder = producer.create_message()
                .with_content(msg);
            if let Some(event_time) = self.event_time {
                builder = builder.event_time(event_time);
            }
            let r = builder.send().await?.await?;
            debug!("sent message: {:?}", r);
        } else {
            loop {
                let mut line = String::new();
                let size = stdin().read_line(&mut line)?;
                if size == 0 {
                    break;
                }
                producer.send(line.trim()).await?;
            }
            producer.send_batch().await?;
        }

        Ok(())
    }
}
