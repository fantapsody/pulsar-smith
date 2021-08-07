use crate::admin::topics::PulsarAdminTopics;
use reqwest::{Client, RequestBuilder};
use std::error::Error;


#[derive(Debug, Clone)]
pub struct  PulsarAdmin {
    service_url: String,
}

impl PulsarAdmin {
    pub fn new(service_url: String) -> PulsarAdmin {
        PulsarAdmin {
            service_url,
        }
    }

    pub(crate) fn get(&self, p: &str) -> Result<RequestBuilder, Box<dyn Error>> {
        let mut builder = Client::builder();

        Ok(builder.build()?
            .get(self.service_url.clone() + p))
    }

    pub fn topics(&self) -> PulsarAdminTopics {
        PulsarAdminTopics {
            admin: self.clone(),
        }
    }
}
