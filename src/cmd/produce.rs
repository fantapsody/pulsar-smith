use std::io::stdin;

use async_trait::async_trait;
use clap::Clap;
use pulsar::message::proto::CompressionType;
use pulsar::message::proto::CompressionType::{Lz4, Snappy, Zlib, Zstd};
use pulsar::ProducerOptions;

use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct ProduceOpts {
    pub topic: String,

    #[clap(long)]
    pub name: Option<String>,

    #[clap(short = 'm', long)]
    pub message: Option<String>,

    #[clap(long, default_value = "1024")]
    pub batch_size: u32,

    #[clap(long)]
    pub compression: Option<String>,
}

impl ProduceOpts {
    fn parse_batch_size(&self) -> Option<u32> {
        if self.message.is_some() {
            None
        } else {
            Some(self.batch_size)
        }
    }

    fn parse_compression(&self) -> Result<Option<CompressionType>, Error> {
        match self.compression.as_ref() {
            Some(str) => {
                match str.to_lowercase().as_str() {
                    "lz4" => Ok(Some(Lz4)),
                    "zlib" => Ok(Some(Zlib)),
                    "zstd" => Ok(Some(Zstd)),
                    "snappy" => Ok(Some(Snappy)),
                    _ => Err(Error::Custom(format!("illegal compression [{}]", str))),
                }
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl AsyncCmd for ProduceOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let options = ProducerOptions {
            encrypted: None,
            metadata: Default::default(),
            schema: None,
            batch_size: self.parse_batch_size(),
            compression: self.parse_compression()?,
        };
        let mut producer = pulsar_ctx.client().await?
            .producer()
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
                producer.send(line.trim()).await?;
            }
            producer.send_batch().await?;
        }

        Ok(())
    }
}
