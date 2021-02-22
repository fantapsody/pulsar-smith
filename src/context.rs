use std::rc::Rc;
use pulsar::{Pulsar, TokioExecutor, Authentication};
use std::error::Error;
use crate::config::PulsarConfig;

pub struct PulsarContext {
    config: PulsarConfig,

    client: Option<Rc<Pulsar<TokioExecutor>>>,
}

impl From<PulsarConfig> for PulsarContext {
    fn from(cfg: PulsarConfig) -> Self {
        PulsarContext {
            config: cfg,
            client: None,
        }
    }
}

impl PulsarContext {
    pub async fn client(&mut self) -> Result<Rc<Pulsar<TokioExecutor>>, Box<dyn Error>> {
        if self.client.is_none() {
            let mut builder = Pulsar::builder(self.config.url.clone(), TokioExecutor);
            if let Some(auth_data) = self.config.auth_params.as_ref() {
                builder = builder.with_auth(Authentication {
                    name: self.config.auth_name.as_ref().unwrap_or(&String::from("token")).clone(),
                    data: auth_data.clone().into_bytes(),
                });
            }
            self.client = Some(Rc::new(builder.build().await?));
            info!("created a new pulsar client");
        }
        Ok(self.client.as_ref().unwrap().clone())
    }
}
