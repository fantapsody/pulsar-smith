use pulsar::message::proto::CompressionType;
use crate::error::Error;
use pulsar::message::proto::CompressionType::{Lz4, Zlib, Zstd, Snappy};
use pulsar::{ProducerOptions, TokioExecutor, Pulsar};
use pulsar::producer::ProducerBuilder;
use clap::Clap;

#[derive(Clap, Debug, Clone)]
pub struct ProducerOpts {
    pub topic: String,

    #[clap(long)]
    pub name: Option<String>,

    #[clap(short = 'b', long, default_value = "0")]
    pub batch_size: i32,

    #[clap(long)]
    pub compression: Option<String>,

    #[clap(long, default_value = "10")]
    pub batching_max_publish_latency_ms: u64,
}

impl ProducerOpts {
    fn parse_batch_size(&self) -> Option<u32> {
        if self.batch_size <= 0 {
            None
        } else {
            Some(self.batch_size as u32)
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

    pub fn producer_builder(&self, client: &Pulsar<TokioExecutor>) -> Result<ProducerBuilder<TokioExecutor>, Error> {
        let mut builder = client.producer()
            .with_topic(self.topic.clone())
            .with_options(ProducerOptions {
                encrypted: None,
                metadata: Default::default(),
                schema: None,
                batch_size: self.parse_batch_size(),
                compression: self.parse_compression()?,
                access_mode: Some(1),
            });
        if let Some(name) = &self.name {
            builder = builder.with_name(name);
        }
        Ok(builder)
    }
}
