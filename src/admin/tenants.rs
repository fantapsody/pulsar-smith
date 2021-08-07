use std::error::Error;

use crate::admin::admin::PulsarAdmin;

pub struct PulsarAdminTenants {
    pub(crate) admin: PulsarAdmin,
}

impl PulsarAdminTenants {
    pub async fn list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get("/admin/v2/tenants")?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
