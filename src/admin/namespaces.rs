use std::error::Error;

use crate::admin::admin::PulsarAdmin;

pub struct PulsarAdminNamespaces {
    pub(crate) admin: PulsarAdmin,
}

impl PulsarAdminNamespaces {
    pub async fn list(&self, tenant: &str) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get(format!("/admin/v2/namespaces/{}", tenant).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
