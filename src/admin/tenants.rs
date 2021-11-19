use std::error::Error;

use crate::admin::admin::PulsarAdmin;
use serde::Serialize;

pub struct PulsarAdminTenants {
    pub(crate) admin: PulsarAdmin,
}

#[derive(Serialize, Debug)]
pub struct TenantInfo {
    #[serde(rename = "adminRoles")]
    pub admin_roles: Vec<String>,

    #[serde(rename = "allowedClusters")]
    pub allowed_clusters: Vec<String>,
}

impl PulsarAdminTenants {
    pub async fn create(&self, tenant: &str, info: &TenantInfo) -> Result<(), Box<dyn Error>> {
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
}
