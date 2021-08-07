use std::error::Error;
use std::sync::Mutex;

use pulsar::{Authentication, Pulsar, TokioExecutor};

use crate::admin::admin::PulsarAdmin;
use crate::auth::auth::Authn;
use crate::config::PulsarConfig;

pub struct PulsarContext {
    mutex: Mutex<()>,

    config: PulsarConfig,

    client: Option<Box<Pulsar<TokioExecutor>>>,
}

impl From<PulsarConfig> for PulsarContext {
    fn from(cfg: PulsarConfig) -> Self {
        PulsarContext {
            mutex: Mutex::new(()),
            config: cfg,
            client: None,
        }
    }
}

impl PulsarContext {
    pub async fn client(&mut self) -> Result<&Pulsar<TokioExecutor>, Box<dyn Error>> {
        let _guard = self.mutex.lock();
        if self.client.is_none() {
            let mut builder = Pulsar::builder(self.config.url.clone(), TokioExecutor);
            if let Some(auth_data) = self.config.auth_params.as_ref() {
                builder = builder.with_auth(Authentication {
                    name: self.config.auth_name.as_ref().unwrap_or(&String::from("token")).clone(),
                    data: auth_data.clone().into_bytes(),
                });
            }
            builder = builder.with_allow_insecure_connection(self.config.allow_insecure_connection);
            self.client = Some(Box::new(builder.build().await?));
            info!("created a new pulsar client");
        }
        Ok(self.client.as_ref().unwrap())
    }

    pub async fn admin(&mut self) -> Result<PulsarAdmin, Box<dyn Error>> {
        Ok(PulsarAdmin::new(self.config.admin_url.clone(),
                            self.config.auth_name.clone(),
                            self.config.auth_params.clone()))
    }

    pub fn authn(&self) -> Result<Box<dyn Authn>, Box<dyn Error>> {
        crate::auth::auth::create(self.config.auth_name.clone().expect("auth name is not provided"),
                                  self.config.auth_params.clone().expect("auth params is not provided"))
    }
}
