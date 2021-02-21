use std::error::Error;
use crate::context::PulsarContext;
use clap::Clap;
use std::io::stdin;
use pulsar::ProducerOptions;

#[derive(Clap, Debug, Clone)]
pub struct ProduceOpts {
    pub topic: String,

    #[clap(long)]
    pub name: Option<String>,

    #[clap(short = 'm', long)]
    pub message: Option<String>,
}

impl ProduceOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
        let options = ProducerOptions {
            encrypted: None,
            metadata: Default::default(),
            schema: None,
            batch_size: None,
            compression: None
        };
        let mut producer = pulsar_ctx.client().await?.producer()
            .with_topic(self.topic.clone())
            .with_name(self.name.as_ref().unwrap_or(&String::from("smith")).clone())
            .with_options(options)
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
                let r = producer.send(line.trim()).await?.await?;
                debug!("sent message: {:?}", r);
            }
        }

        Ok(())
    }
}
