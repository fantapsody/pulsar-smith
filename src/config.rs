use crate::error::Error;
use std::fs;
use serde::{Serialize, Deserialize};

const CONFIG_PATH: &str = ".pulsar-smith/config";

pub struct PulsarConfig {
    pub url: String,

    pub admin_url: String,

    pub auth_name: Option<String>,

    pub auth_params: Option<String>,

    pub allow_insecure_connection: bool,
}

impl PulsarConfig {
}

impl Configs {
    pub fn load()-> Result<Configs, Error> {
        #![allow(deprecated)]
        if let Some(home_dir) = std::env::home_dir() {
            let config_path = home_dir.join(CONFIG_PATH);
            let str = fs::read_to_string(config_path)?;
            let cfg: Configs = serde_yaml::from_str(str.as_str())?;
            Ok(cfg)
        } else {
            Err(Error::Custom(format!("cannot determine home dir")))
        }
    }

    pub fn get_pulsar_config(&self, context_name: &str) -> Result<PulsarConfig, Error> {
        if let Some(context_item) = self.contexts.iter().find(|p| {p.name == context_name}) {
            if let Some(cluster_item) = self.clusters.iter().find(|p| {p.name == context_item.context.cluster}) {
                if let Some(user_item) = self.users.iter().find(|p| p.name == context_item.context.user) {
                    let user = &user_item.user;
                    Ok(PulsarConfig {
                        url: cluster_item.cluster.url.clone(),
                        admin_url: cluster_item.cluster.admin_service_url.clone()
                            .expect(format!("admin-url not defined for cluster [{}]", cluster_item.name.as_str()).as_str()),
                        allow_insecure_connection: cluster_item.cluster.allow_insecure_connection,
                        auth_name: user.auth_name.clone(),
                        auth_params: user.auth_params.clone(),
                    })
                } else {
                    Err(Error::Custom(format!("context [{}] not exist", context_name)))
                }
            } else {
                Err(Error::Custom(format!("context [{}] not exist", context_name)))
            }
        } else {
            Err(Error::Custom(format!("context [{}] not exist", context_name)))
        }
    }

    pub fn has_current_context(&self) -> bool {
        return self.current_context.is_some()
    }

    pub fn get_current_pulsar_config(&self) -> Result<PulsarConfig, Error> {
        match &self.current_context {
            Some(current) => self.get_pulsar_config(current.as_str()),
            None => Err(Error::Custom("current context not set".to_string())),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Configs {
    clusters: Vec<ClusterItem>,
    users: Vec<UserItem>,
    contexts: Vec<ContextItem>,
    #[serde(rename = "current-context")]
    current_context: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ClusterItem {
    name: String,
    cluster: Cluster,
}

#[derive(Serialize, Deserialize)]
pub struct Cluster {
    url: String,
    #[serde(rename = "admin-url", default)]
    admin_service_url: Option<String>,
    #[serde(rename = "allow-insecure-connection", default)]
    allow_insecure_connection: bool,
}

#[derive(Serialize, Deserialize)]
pub struct UserItem {
    name: String,
    user: User,
}

#[derive(Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "auth-name")]
    auth_name: Option<String>,
    #[serde(rename = "auth-params")]
    auth_params: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ContextItem {
    name: String,
    context: Context,
}

#[derive(Serialize, Deserialize)]
pub struct Context {
    cluster: String,
    user: String,
}
