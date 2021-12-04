use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminNamespaces {
    pub(crate) admin: PulsarAdmin,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct NamespacePolicies {
    pub bundles: Option<BundlesData>,

    pub replication_clusters: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistencePolicies>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BundlesData {
    #[serde(rename = "numBundles")]
    pub num_bundles: u64,

    #[serde(rename = "boundaries")]
    pub boundaries: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PersistencePolicies {
    #[serde(rename = "bookkeeperEnsemble")]
    pub bookkeeper_ensemble: i32,

    #[serde(rename = "bookkeeperWriteQuorum")]
    pub bookkeeper_write_quorum: i32,

    #[serde(rename = "bookkeeperAckQuorum")]
    pub bookkeeper_ack_quorum: i32,

    #[serde(rename = "managedLedgerMaxMarkDeleteRate")]
    pub managed_ledger_max_mark_delete_rate: f64,
}

impl PulsarAdminNamespaces {
    pub async fn list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v2/namespaces/{}", namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }


    pub async fn create(&self, namespace: &str, policies: &NamespacePolicies) -> Result<(), Error> {
        let res = self.admin.put(format!("/admin/v2/namespaces/{}", namespace).as_str())?
            .json(policies)
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(res.text().await?.into())
        }
    }

    pub async fn policies(&self, namespace: &str) -> Result<NamespacePolicies, Error> {
        let body = self.admin.get(format!("/admin/v2/namespaces/{}", namespace).as_str())?
            .send().await?
            .text().await?;
        debug!("{}", body.as_str());
        Ok(serde_json::from_str(body.as_str())?)
    }

    pub async fn permissions(&self, namespace: &str) -> Result<HashMap<String, Vec<String>>, Error> {
        let res = self.admin.get(format!("/admin/v2/namespaces/{}/permissions", namespace).as_str())?
            .send().await?;
        let body = res.text().await?;
        debug!("{}", body.as_str());

        Ok(serde_json::from_str::<HashMap<String, Vec<Option<String>>>>(body.as_str())?.into_iter()
            .map(|e| {
                let values: Vec<String> = e.1.into_iter()
                    .filter(|op| op.is_some())
                    .map(|op| op.unwrap())
                    .collect();
                (e.0, values)
            })
            .collect())
    }

    pub async fn grant_permission(&self, namespace: &str, role: &str, permissions: &Vec<String>) -> Result<(), Error> {
        let res = self.admin.post(format!("/admin/v2/namespaces/{}/permissions/{}", namespace, role).as_str())?
            .json(permissions)
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(res.text().await?.into())
        }
    }

    pub async fn revoke_permission(&self, namespace: &str, role: &str) -> Result<(), Error> {
        let res = self.admin.delete(format!("/admin/v2/namespaces/{}/permissions/{}", namespace, role).as_str())?
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(res.text().await?.into())
        }
    }

    pub async fn update_persistence(&self, namespace: &str, persistence: &PersistencePolicies) -> Result<(), Error> {
        let res = self.admin.post(format!("/admin/v2/namespaces/{}/persistence", namespace).as_str())?
            .json(persistence)
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(res.text().await?.into())
        }
    }

    pub async fn remove_persistence(&self, namespace: &str) -> Result<(), Error> {
        let res = self.admin.delete(format!("/admin/v2/namespaces/{}/persistence", namespace).as_str())?
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(res.text().await?.into())
        }
    }
}
