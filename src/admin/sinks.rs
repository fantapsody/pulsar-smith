use serde::{Deserialize, Serialize};

use crate::admin::admin::PulsarAdmin;
use crate::admin::error::Error;

pub struct PulsarAdminSinks<'a> {
    pub(crate) admin: &'a PulsarAdmin,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SinkDef {
    name: String,
    description: String,
    id: String,
    version: String,
    #[serde(rename = "imageRepository")]
    image_repository: Option<String>,
    #[serde(rename = "imageTag")]
    image_tag: Option<String>,
    #[serde(rename = "typeClassName")]
    type_class_name: Option<String>,
    #[serde(rename = "sourceClass")]
    source_class: Option<String>,
    #[serde(rename = "sinkClass")]
    sink_class: Option<String>,
    #[serde(rename = "sourceConfigClass")]
    source_config_class: Option<String>,
    #[serde(rename = "sinkConfigClass")]
    sink_config_class: Option<String>,
    #[serde(rename = "sourceTypeClassName")]
    source_type_class_name: Option<String>,
    #[serde(rename = "sinkTypeClassName")]
    sink_type_class_name: Option<String>,
    #[serde(rename = "defaultSchemaType")]
    default_schema_type: Option<String>,
    jar: String,
}

impl<'a> PulsarAdminSinks<'a> {
    pub async fn list(&self, namespace: &str) -> Result<Vec<String>, Error> {
        Ok(self.admin.get(format!("/admin/v3/sinks/{}", namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }

    pub async fn builtin_sinks(&self) -> Result<Vec<SinkDef>, Error> {
        let res = self.admin.get("/admin/v3/sinks/builtinsinks")?
            .send().await?;
        let body = res.text().await?;
        debug!("got response [{}]", body);
        Ok(serde_json::from_str(body.as_str())?)
    }
}
