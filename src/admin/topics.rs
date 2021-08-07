use std::error::Error;

use crate::admin::admin::PulsarAdmin;
use crate::admin::topics::TopicDomain::{NonPersistent, Persistent};

pub struct PulsarAdminTopics {
    pub(crate) admin: PulsarAdmin,
}

pub enum TopicDomain {
    Persistent,
    NonPersistent,
}

impl ToString for TopicDomain {
    #[inline]
    fn to_string(&self) -> String {
        match self {
            Persistent => "persistent",
            TopicDomain::NonPersistent => "non-persistent",
        }.to_string()
    }
}

impl TopicDomain {
    pub fn parse(name: &str) -> Result<TopicDomain, Box<dyn Error>> {
        match name.to_uppercase().as_str() {
            "PERSISTENT" => Ok(Persistent),
            "NON_PERSISTENT" => Ok(NonPersistent),
            &_ => Err(Box::<dyn Error>::from(format!("invalid domain name [{}]", name)))
        }
    }
}

impl PulsarAdminTopics {
    pub async fn list(&self, namespace: &str, domain: TopicDomain) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(self.admin.get(format!("/admin/v2/{}/{}", domain.to_string(), namespace).as_str())?
            .send().await?
            .json::<Vec<String>>().await?)
    }
}
