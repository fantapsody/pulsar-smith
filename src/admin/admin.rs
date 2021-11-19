use std::error::Error;

use reqwest::{Client, ClientBuilder, header, RequestBuilder};

use crate::admin::namespaces::PulsarAdminNamespaces;
use crate::admin::tenants::PulsarAdminTenants;
use crate::admin::topics::PulsarAdminTopics;
use crate::admin::clusters::PulsarAdminClusters;

#[derive(Debug, Clone)]
pub struct PulsarAdmin {
    service_url: String,
    auth_name: Option<String>,
    auth_params: Option<String>,
}

impl PulsarAdmin {
    pub fn new(service_url: String, auth_name: Option<String>, auth_params: Option<String>) -> PulsarAdmin {
        PulsarAdmin {
            service_url,
            auth_name,
            auth_params,
        }
    }

    fn client_builder(&self) -> Result<ClientBuilder, Box<dyn Error>> {
        let mut builder = Client::builder();
        if self.auth_name.is_some() && self.auth_params.is_some() {
            if self.auth_name.as_ref().unwrap() == "token" {
                let mut headers = header::HeaderMap::new();
                headers.insert("Authorization", header::HeaderValue::from_str(format!("Bearer {}", self.auth_params.as_ref().unwrap()).as_str())?);
                builder = builder.default_headers(headers);
            }
        }
        Ok(builder)
    }

    pub(crate) fn put(&self, p: &str) -> Result<RequestBuilder, Box<dyn Error>> {
        Ok(self.client_builder()?
            .build()?
            .put(self.service_url.clone() + p))
    }

    pub(crate) fn get(&self, p: &str) -> Result<RequestBuilder, Box<dyn Error>> {
        Ok(self.client_builder()?
            .build()?
            .get(self.service_url.clone() + p))
    }

    pub(crate) fn post(&self, p: &str) -> Result<RequestBuilder, Box<dyn Error>> {
        Ok(self.client_builder()?
            .build()?
            .post(self.service_url.clone() + p))
    }

    pub(crate) fn delete(&self, p: &str) -> Result<RequestBuilder, Box<dyn Error>> {
        Ok(self.client_builder()?
            .build()?
            .delete(self.service_url.clone() + p))
    }

    pub fn clusters(&self) -> PulsarAdminClusters {
        PulsarAdminClusters {
            admin: self.clone(),
        }
    }

    pub fn tenants(&self) -> PulsarAdminTenants {
        PulsarAdminTenants {
            admin: self.clone(),
        }
    }

    pub fn namespaces(&self) -> PulsarAdminNamespaces {
        PulsarAdminNamespaces {
            admin: self.clone(),
        }
    }

    pub fn topics(&self) -> PulsarAdminTopics {
        PulsarAdminTopics {
            admin: self.clone(),
        }
    }
}
