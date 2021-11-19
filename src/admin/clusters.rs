use crate::admin::admin::PulsarAdmin;
use std::error::Error;

pub struct PulsarAdminClusters {
    pub(crate) admin: PulsarAdmin,
}

impl PulsarAdminClusters {
    pub async fn list(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get("/admin/v2/clusters")?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
