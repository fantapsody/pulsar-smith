use crate::error::Error;
use pulsar::{ProducerOptions, TokioExecutor, Pulsar};
use pulsar::producer::ProducerBuilder;
use clap::Parser;
use pulsar::compression::{Compression};

#[derive(Parser, Debug, Clone)]
#[command(version = "1.0", author = "Yang Yang <yyang@streamnative.io>")]
pub struct ProducerOpts {
    pub topic: String,

    #[arg(long)]
    pub name: Option<String>,

    #[arg(short = 'b', long, default_value = "0")]
    pub batch_size: i32,

    #[arg(long)]
    pub compression: Option<String>,

    #[arg(long, default_value = "10")]
    pub batching_max_publish_latency_ms: u64,
}

impl ProducerOpts {
    pub fn parse_batch_size(&self) -> Option<u32> {
        if self.batch_size <= 0 {
            None
        } else {
            Some(self.batch_size as u32)
        }
    }

    fn parse_compression(&self) -> Result<Option<Compression>, Error> {
        match self.compression.as_ref() {
            Some(str) => {
                match str.to_lowercase().as_str() {
                    "lz4" => Ok(Some(Compression::Lz4(Default::default()))),
                    "zlib" => Ok(Some(Compression::Zlib(Default::default()))),
                    "zstd" => Ok(Some(Compression::Zstd(Default::default()))),
                    "snappy" => Ok(Some(Compression::Snappy(Default::default()))),
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
                access_mode: Some(0),
            });
        if let Some(name) = &self.name {
            builder = builder.with_name(name);
        }
        Ok(builder)
    }
}
