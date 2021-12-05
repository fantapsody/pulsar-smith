use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminClusters<'a> {
    pub(crate) admin: &'a PulsarAdmin,
}

impl<'a> PulsarAdminClusters<'a> {
    pub async fn list(&self) -> Result<Vec<String>, Error> {
        Ok(self.admin.get("/admin/v2/clusters")?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
