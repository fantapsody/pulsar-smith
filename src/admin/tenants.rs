use std::error::Error;

use serde::{Deserialize, Serialize};

use crate::admin::admin::PulsarAdmin;

pub struct PulsarAdminTenants {
    pub(crate) admin: PulsarAdmin,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantInfo {
    #[serde(rename = "adminRoles")]
    pub admin_roles: Vec<String>,

    #[serde(rename = "allowedClusters")]
    pub allowed_clusters: Vec<String>,
}

impl PulsarAdminTenants {
    pub async fn create(&self, tenant: &str, mut info: TenantInfo) -> Result<(), Box<dyn Error>> {
        if info.allowed_clusters.is_empty() {
            let clusters = self.admin.clusters().list().await?;
            if clusters.len() == 1 {
                info.allowed_clusters.push(clusters[0].clone());
            }
        }
        let r = self.admin.put(format!("/admin/v2/tenants/{}", tenant).as_str())?
            .json(&info)
            .send().await?;
        debug!("Got {:?}", &r);
        if r.status().is_success() {
            Ok(())
        } else {
            Err(Box::<dyn Error>::from(r.text().await?))
        }
    }

    pub async fn list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get("/admin/v2/tenants")?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn get(&self, tenant: &str) -> Result<TenantInfo, Box<dyn Error>> {
        Ok(self.admin.get(format!("/admin/v2/tenants/{}", tenant).as_str())?
            .send().await?
            .json::<TenantInfo>().await?)
    }
}
