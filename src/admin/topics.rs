use std::collections::HashMap;

use reqwest::header::{CONTENT_TYPE, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminTopics<'a> {
    pub(crate) admin: &'a PulsarAdmin,
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
    pub fn parse(name: &str) -> Result<TopicDomain, Error> {
        match name.to_uppercase().replace("-", "_").as_str() {
            "PERSISTENT" => Ok(TopicDomain::Persistent),
            "NON_PERSISTENT" => Ok(TopicDomain::NonPersistent),
            &_ => Err(format!("invalid domain name [{}]", name).into())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupResponse {
    #[serde(rename = "brokerUrl")]
    pub broker_url: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStats {
    count: Option<i64>,
    #[serde(rename = "msgRateIn")]
    msg_rate_in: f64,
    #[serde(rename = "msgRateOut")]
    msg_rate_out: f64,
    #[serde(rename = "msgThroughputIn")]
    msg_throughput_in: f64,
    #[serde(rename = "msgThroughputOut")]
    msg_throughput_out: f64,
    #[serde(rename = "msgInCounter")]
    msg_in_counter: i64,
    #[serde(rename = "msgOutCounter")]
    msg_out_counter: i64,
    #[serde(rename = "bytesInCounter")]
    bytes_in_counter: i64,
    #[serde(rename = "bytesOutCounter")]
    bytes_out_counter: i64,
    #[serde(rename = "averageMsgSize")]
    average_msg_size: f64,
    #[serde(rename = "msgChunkPublished")]
    msg_chunk_published: bool,
    #[serde(rename = "storageSize")]
    storage_size: i64,
    #[serde(rename = "backlogSize")]
    backlog_size: i64,
    #[serde(rename = "offloadedStorageSize")]
    offloaded_storage_size: i64,
    #[serde(rename = "waitingPublishers")]
    waiting_publishers: i64,
}

impl<'a> PulsarAdminTopics<'a> {
    pub async fn list(&self, namespace: &str, domain: TopicDomain) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v2/{}/{}", domain.to_string(), namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn list_partitioned(&self, namespace: &str, domain: TopicDomain) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v2/{}/{}/partitioned", domain.to_string(), namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn lookup(&self, topic: &str) -> Result<LookupResponse, Error> {
        let canonical_topic = topic.replace("://", "/");
        let body = self.admin.get(format!("/lookup/v2/topic/{}", canonical_topic).as_str())?
            .send().await?
            .text().await?;
        debug!("Lookup response: {}", body);
        Ok(serde_json::from_str(body.as_str())?)
    }

    pub async fn stats(&self, topic: &str,
                       get_precise_backlog: bool,
                       subscription_backlog_size: bool) -> Result<serde_json::Value, Error> {
        let canonical_topic = topic.replace("://", "/");
        let body = self.admin.get(format!("/admin/v2/{}/stats", canonical_topic).as_str())?
            .query(&[
                ("getPreciseBacklog", get_precise_backlog),
                ("subscriptionBacklogSize", subscription_backlog_size)
            ])
            .send().await?
            .text().await?;
        debug!("Stats response:\n{}", body);
        Ok(serde_json::from_str(body.as_str())?)
    }

    pub async fn permissions(&self, topic: &str) -> Result<HashMap<String, Vec<Option<String>>>, Error> {
        let canonical_topic = topic.replace("://", "/");
        Ok(self.admin.get(format!("/admin/v2/{}/permissions", canonical_topic).as_str())?
            .send().await?
            .json().await?)
    }

    pub async fn grant_permissions(&self, topic: &str, role: &str, permissions: &[String]) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");
        let resp = self.admin.post(format!("/admin/v2/{}/permissions/{}", canonical_topic, role).as_str())?
            .json(&permissions)
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            resp.error_for_status()?;
            Ok(())
        }
    }

    pub async fn revoke_permissions(&self, topic: &str, role: &str) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");
        let resp = self.admin.delete(format!("/admin/v2/{}/permissions/{}", canonical_topic, role).as_str())?
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            resp.error_for_status()?;
            Ok(())
        }
    }

    pub async fn create_non_partitioned_topic(&self, topic: &str) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");
        let resp = self.admin
            .put(format!("/admin/v2/{}", canonical_topic).as_str())?
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.text().await?.into())
        }
    }

    pub async fn create_partitioned_topic(&self, topic: &str, num_partitions: i32) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");
        let resp = self.admin
            .put(format!("/admin/v2/{}/partitions", canonical_topic).as_str())?
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(num_partitions.to_string())
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.text().await?.into())
        }
    }

    pub async fn delete_topic(&self, topic: &str, force: bool, delete_schema: bool) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");
        let resp = self.admin
            .delete(format!("/admin/v2/{}", canonical_topic).as_str())?
            .query(&[("force", force.to_string()), ("", delete_schema.to_string())])
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.text().await?.into())
        }
    }

    pub async fn delete_partitioned_topic(&self, topic: &str, force: bool, delete_schema: bool) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");
        let resp = self.admin
            .delete(format!("/admin/v2/{}/partitions", canonical_topic).as_str())?
            .query(&[("force", force.to_string()), ("", delete_schema.to_string())])
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.text().await?.into())
        }
    }

    pub async fn subscriptions(&self, topic: &str) -> Result<Vec<String>, Error> {
        let canonical_topic = topic.replace("://", "/");
        Ok(self.admin.get(format!("/admin/v2/{}/subscriptions", canonical_topic).as_str())?
            .send().await?
            .json().await?)
    }

    pub async fn unsubscribe(&self, topic: &str, subscription: &str, force: bool) -> Result<(), Error> {
        let canonical_topic = topic.replace("://", "/");

        let resp = self.admin.delete(format!("/admin/v2/{}/subscription/{}", canonical_topic,
                                             urlencoding::encode(subscription)).as_str())?
            .query(&[("force", force.to_string())])
            .send().await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.text().await?.into())
        }
    }
}
