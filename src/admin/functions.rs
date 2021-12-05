use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminFunctions<'a> {
    pub(crate) admin: &'a PulsarAdmin,
}

impl<'a> PulsarAdminFunctions<'a> {
    pub async fn list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v3/functions/{}", namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
