use std::io::stdin;

use async_trait::async_trait;
use clap::Clap;

use crate::cmd::cmd::AsyncCmd;
use crate::cmd::commons::ProducerOpts;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct ProduceOpts {
    #[clap(flatten)]
    pub producer_opts: ProducerOpts,

    #[clap(short = 'm', long)]
    pub message: Option<String>,
}

#[async_trait]
impl AsyncCmd for ProduceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let mut producer = self.producer_opts.producer_builder(pulsar_ctx.client().await?)?
            .build()
            .await?;

        if let Some(msg) = &self.message {
            let r = producer.send(msg).await?.await?;
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
