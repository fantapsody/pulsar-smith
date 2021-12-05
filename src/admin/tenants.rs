use serde::{Deserialize, Serialize};

use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminTenants<'a> {
    pub(crate) admin: &'a PulsarAdmin,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantInfo {
    #[serde(rename = "adminRoles")]
    pub admin_roles: Vec<String>,

    #[serde(rename = "allowedClusters")]
    pub allowed_clusters: Vec<String>,
}

impl<'a> PulsarAdminTenants<'a> {
    pub async fn create(&self, tenant: &str, mut info: TenantInfo) -> Result<(), Error> {
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
            Err(r.text().await?.into())
        }
    }

    pub async fn list(&self) -> Result<Vec<String>, Error> {
        Ok(self.admin.get("/admin/v2/tenants")?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn get(&self, tenant: &str) -> Result<TenantInfo, Error> {
        Ok(self.admin.get(format!("/admin/v2/tenants/{}", tenant).as_str())?
            .send().await?
            .json::<TenantInfo>().await?)
    }
}
