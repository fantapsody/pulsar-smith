use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminFunctions {
    pub(crate) admin: PulsarAdmin,
}

impl PulsarAdminFunctions {
    pub async fn list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v3/functions/{}", namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
