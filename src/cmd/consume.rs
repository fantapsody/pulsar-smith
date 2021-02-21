use std::error::Error;
use crate::context::PulsarContext;
use clap::Clap;
use futures::TryStreamExt;
use pulsar::{Consumer, ConsumerOptions, SubType};
use pulsar::message::proto::command_subscribe::SubType::{Exclusive, Failover, Shared};
use std::u64::MAX;
use pulsar::message::proto::command_subscribe::InitialPosition;
use pulsar::message::proto::command_subscribe::InitialPosition::{Earliest, Latest};

#[derive(Clap, Debug, Clone)]
pub struct ConsumeOpts {
    pub topic: String,

    #[clap(long)]
    pub name: Option<String>,

    #[clap(short = 'n', long)]
    pub num: Option<u64>,

    #[clap(short = 't', long, default_value = "Exclusive")]
    pub subscription_type: String,

    #[clap(short = 's', long, default_value = "test")]
    pub subscription_name: String,

    #[clap(short = 'p', long, default_value = "Latest")]
    pub subscription_position: String,
}

impl ConsumeOpts {
    fn parse_sub_type(t: &str) -> Result<SubType, Box<dyn Error>> {
        match t.to_lowercase().as_str() {
            "exclusive" => Ok(Exclusive),
            "shared" => Ok(Shared),
            "failover" => Ok(Failover),
            _ => Err(Box::<dyn Error>::from(format!("illegal subscription type [{}]", t))),
        }
    }

    fn parse_subscription_position(t: &str) -> Result<InitialPosition, Box<dyn Error>> {
        match t.to_lowercase().as_str() {
            "earliest" => Ok(Earliest),
            "latest" => Ok(Latest),
            _ => Err(Box::<dyn Error>::from(format!("illegal initial position [{}]", t))),
        }
    }

    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Box<dyn Error>> {
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
                initial_position: Some(Self::parse_subscription_position(self.subscription_position.as_str())? as i32),
            })
            .build()
            .await?;
        let mut counter = 0u64;
        while let Some(msg) = consumer.try_next().await? {
            consumer.ack(&msg).await?;
            debug!("got message, topic: [{}], metadata: [{:?}], data: [{:?}]", &msg.topic, &msg.payload.metadata, &msg.payload.data);
            println!("{}", String::from_utf8(msg.payload.data)?);
            counter += 1;
            if self.num.unwrap_or(MAX) <= counter {
                break;
            }
        }

        Ok(())
    }
}
