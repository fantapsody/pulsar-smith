use async_trait::async_trait;
use clap::Parser;
use futures::TryStreamExt;
use pulsar::{Consumer, ConsumerOptions, SubType};
use pulsar::consumer::{InitialPosition, Message};

use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::TimeZone;

#[derive(Parser, Debug, Clone)]
pub struct ConsumeOpts {
    pub topic: String,

    #[arg(long)]
    pub name: Option<String>,

    #[arg(short = 'n', long)]
    pub num: Option<u64>,

    #[arg(short = 't', long, default_value = "Exclusive")]
    pub subscription_type: String,

    #[arg(short = 's', long, default_value = "test")]
    pub subscription_name: String,

    #[arg(short = 'p', long, default_value = "Latest")]
    pub subscription_position: String,
}

impl ConsumeOpts {
    fn parse_sub_type(t: &str) -> Result<SubType, Error> {
        match t.to_lowercase().as_str() {
            "exclusive" => Ok(SubType::Exclusive),
            "shared" => Ok(SubType::Shared),
            "failover" => Ok(SubType::Failover),
            _ => Err(Error::Custom(format!("illegal subscription type [{}]", t))),
        }
    }

    fn parse_subscription_position(t: &str) -> Result<InitialPosition, Error> {
        match t.to_lowercase().as_str() {
            "earliest" => Ok(InitialPosition::Earliest),
            "latest" => Ok(InitialPosition::Latest),
            _ => Err(Error::Custom(format!("illegal initial position [{}]", t))),
        }
    }
}

#[async_trait]
impl AsyncCmd for ConsumeOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let mut consumer: Consumer<String, _> = pulsar_ctx.client().await?
            .consumer()
            .with_topic(self.topic.clone())
            .with_consumer_name(self.name.as_ref().unwrap_or(&String::from("smith")).clone())
            .with_subscription(self.subscription_name.clone())
            .with_subscription_type(Self::parse_sub_type(self.subscription_type.as_str())?)
            .with_options(ConsumerOptions {
                priority_level: None,
                durable: None,
                start_message_id: None,
                metadata: Default::default(),
                read_compacted: Some(true),
                schema: None,
                initial_position: Self::parse_subscription_position(self.subscription_position.as_str())?,
            })
            .build()
            .await?;
        let mut counter = 0u64;
        while let Some(msg) = consumer.try_next().await? {
            consumer.ack(&msg).await?;

            self.print_msg(msg);
            counter += 1;
            if self.num.unwrap_or(u64::MAX) <= counter {
                break;
            }
        }

        Ok(())
    }
}

impl ConsumeOpts {
    fn print_msg(&self, msg: Message<String>) {
        debug!("got message, topic: [{}], metadata: [{:?}], data: [{:?}]", &msg.topic, &msg.payload.metadata, &msg.payload.data);
        let latency_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() - msg.metadata().publish_time as u128;
        println!("latency(ms): {}", latency_ms);
        if let Some(time) = msg.metadata().event_time {
            println!("event time(ms): {}", chrono::Utc.timestamp_millis(time as i64).to_rfc3339());
        }
        println!("properties: {:?}", msg.metadata().properties);

        println!("msg:\n{}", String::from_utf8(msg.payload.data).unwrap());
    }
}
