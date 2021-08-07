use std::error::Error;

use serde::{Deserialize, Serialize};

use crate::admin::admin::PulsarAdmin;

pub struct PulsarAdminTopics {
    pub(crate) admin: PulsarAdmin,
}

pub enum TopicDomain {
    Persistent,
    NonPersistent,
}

impl ToString for TopicDomain {
    #[inline]
    fn to_string(&self) -> String {
        match self {
            TopicDomain::Persistent => "persistent",
            TopicDomain::NonPersistent => "non-persistent",
        }.to_string()
    }
}

impl TopicDomain {
    pub fn parse(name: &str) -> Result<TopicDomain, Box<dyn Error>> {
        match name.to_uppercase().replace("-", "_").as_str() {
            "PERSISTENT" => Ok(TopicDomain::Persistent),
            "NON_PERSISTENT" => Ok(TopicDomain::NonPersistent),
            &_ => Err(Box::<dyn Error>::from(format!("invalid domain name [{}]", name)))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupResponse {
    #[serde(rename = "brokerUrl")]
    pub broker_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "brokerUrlTls")]
    pub broker_url_tls: Option<String>,
    #[serde(rename = "httpUrl")]
    pub http_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "httpUrlTls")]
    pub http_url_tls: Option<String>,
    #[serde(rename = "nativeUrl")]
    pub native_url: Option<String>,
}

impl PulsarAdminTopics {
    pub async fn list(&self, namespace: &str, domain: TopicDomain) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get(format!("/admin/v2/{}/{}", domain.to_string(), namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn lookup(&self, topic: &str) -> Result<LookupResponse, Box<dyn Error>> {
        let canonical_topic = topic.replace("://", "/");
        let body = self.admin.get(format!("/lookup/v2/topic/{}", canonical_topic).as_str())?
            .send().await?
            .text().await?;
        debug!("Lookup response: {}", body);
        Ok(serde_json::from_str(body.as_str())?)
    }
}
