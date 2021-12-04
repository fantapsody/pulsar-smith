use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminSinks {
    pub(crate) admin: PulsarAdmin,
}

impl PulsarAdminSinks {
    pub async fn list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v3/sinks/{}", namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn builtin_sinks(&self) -> Result<Vec<String>, Error> {
        Ok(self.admin.get("/admin/v3/sinks/builtinsinks")?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
