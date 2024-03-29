use futures::lock::Mutex;
use pulsar::{Authentication, Pulsar, TokioExecutor};

use crate::admin::admin::PulsarAdmin;
use crate::auth::auth::Authn;
use crate::config::PulsarConfig;
use crate::error::Error;

pub struct PulsarContext {
    mutex: Mutex<()>,

    config: PulsarConfig,

    client: Option<Box<Pulsar<TokioExecutor>>>,

    admin: Option<PulsarAdmin>,
}

impl From<PulsarConfig> for PulsarContext {
    fn from(cfg: PulsarConfig) -> Self {
        PulsarContext {
            mutex: Mutex::new(()),
            config: cfg,
            client: None,
            admin: None,
        }
    }
}

impl PulsarContext {
    pub fn get_config(&self) -> &PulsarConfig {
        &self.config
    }

    pub async fn client(&mut self) -> Result<&Pulsar<TokioExecutor>, Error> {
        let _guard = self.mutex.lock();
        if self.client.is_none() {
            let mut builder = Pulsar::builder(self.config.url.clone(), TokioExecutor);
            if let Some(auth_data) = self.config.auth_params.as_ref() {
                let auth_name = "token";
                let auth = crate::auth::auth::create(self.config.auth_name.as_ref().unwrap().clone(), auth_data.clone())?;
                let auth_data = auth.get_token().await?;
                builder = builder.with_auth(Authentication {
                    name: auth_name.to_string(),
                    data: auth_data.into_bytes(),
                });
            }
            builder = builder.with_allow_insecure_connection(self.config.allow_insecure_connection);
            builder = builder.with_tls_hostname_verification_enabled(self.config.tls_hostname_verification_enabled);
            self.client = Some(Box::new(builder.build().await?));
            info!("created a new pulsar client");
        }
        Ok(self.client.as_ref().unwrap())
    }

    pub async fn new_client(&self) -> Result<Pulsar<TokioExecutor>, Box<dyn std::error::Error>> {
        let mut builder = Pulsar::builder(self.config.url.clone(), TokioExecutor);
        if let Some(auth_data) = self.config.auth_params.as_ref() {
            let auth_name = "token";
            let auth = crate::auth::auth::create(self.config.auth_name.as_ref().unwrap().clone(), auth_data.clone())?;
            let auth_data = auth.get_token().await?;
            builder = builder.with_auth(Authentication {
                name: auth_name.to_string(),
                data: auth_data.into_bytes(),
            });
        }
        builder = builder.with_allow_insecure_connection(self.config.allow_insecure_connection);
        builder = builder.with_tls_hostname_verification_enabled(self.config.tls_hostname_verification_enabled);
        Ok(builder.build().await?)
    }

    pub async fn admin(&mut self) -> Result<&PulsarAdmin, Error> {
        let _guard = self.mutex.lock();
        if self.admin.is_none() {
            let (auth_name, auth_params) = if self.config.auth_name.is_none() {
                (None, None)
            } else {
                let auth = crate::auth::auth::create(self.config.auth_name.as_ref().unwrap().clone(),
                                                     self.config.auth_params.as_ref().unwrap().clone())?;
                (Some("token".to_string()), Some(auth.get_token().await?))
            };
            self.admin = Some(PulsarAdmin::new(self.config.admin_url.clone(),
                                               auth_name,
                                               auth_params));
        }
        Ok(self.admin.as_ref().unwrap())
    }

    pub fn authn(&self) -> Result<Box<dyn Authn>, Error> {
        Ok(crate::auth::auth::create(self.config.auth_name.clone().expect("auth name is not provided"),
                                     self.config.auth_params.clone().expect("auth params is not provided"))?)
    }
}
