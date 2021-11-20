use std::collections::HashMap;
use std::error::Error;
use serde::Serialize;

use crate::admin::admin::PulsarAdmin;

pub struct PulsarAdminNamespaces {
    pub(crate) admin: PulsarAdmin,
}

#[derive(Serialize)]
pub struct NamespacePolicies {
    pub bundles: Option<u64>,

    pub clusters: Option<Vec<String>>,
}

impl PulsarAdminNamespaces {
    pub async fn list(&self, namespace: &str) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get(format!("/admin/v2/namespaces/{}", namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn create(&self, namespace: &str, policies: &NamespacePolicies) -> Result<(), Box<dyn Error>> {
        let res = self.admin.put(format!("/admin/v2/namespaces/{}", namespace).as_str())?
            .json(policies)
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(Box::from(res.text().await?))
        }
    }

    pub async fn permissions(&self, namespace: &str) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
        let res = self.admin.get(format!("/admin/v2/namespaces/{}/permissions", namespace).as_str())?
            .send().await?;
        let body = res.text().await?;
        info!("{}", body.as_str());

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

    pub async fn grant_permission(&self, namespace: &str, role: &str, permissions: &Vec<String>) -> Result<(), Box<dyn Error>> {
        let res = self.admin.post(format!("/admin/v2/namespaces/{}/permissions/{}", namespace, role).as_str())?
            .json(permissions)
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(Box::from(res.text().await?))
        }
    }

    pub async fn revoke_permission(&self, namespace: &str, role: &str) -> Result<(), Box<dyn Error>> {
        let res = self.admin.delete(format!("/admin/v2/namespaces/{}/permissions/{}", namespace, role).as_str())?
            .send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(Box::from(res.text().await?))
        }
    }
}
