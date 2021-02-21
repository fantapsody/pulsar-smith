use std::rc::Rc;
use pulsar::{Pulsar, TokioExecutor, Authentication};
use crate::opts::PulsarOpts;
use std::error::Error;

pub struct PulsarContext {
    opts: PulsarOpts,

    client: Option<Rc<Pulsar<TokioExecutor>>>,
}

impl PulsarContext {
    pub fn create(opts: PulsarOpts) -> PulsarContext {
        PulsarContext {
            opts,
            client: None,
        }
    }

    pub async fn client(&mut self) -> Result<Rc<Pulsar<TokioExecutor>>, Box<dyn Error>> {
        if self.client.is_none() {
            let mut builder = Pulsar::builder(self.opts.url.clone(), TokioExecutor);
            if let Some(auth_data) = self.opts.auth_params.as_ref() {
                builder = builder.with_auth(Authentication {
                    name: self.opts.auth_name.as_ref().unwrap_or(&String::from("token")).clone(),
                    data: auth_data.clone().into_bytes(),
                });
            }
            self.client = Some(Rc::new(builder.build().await?));
            info!("created a new pulsar client");
        }
        Ok(self.client.as_ref().unwrap().clone())
    }
}
