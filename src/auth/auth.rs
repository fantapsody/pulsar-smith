use async_trait::async_trait;

use crate::auth::error::Error;
use crate::auth::oauth2::OAuth2Authn;

#[async_trait]
pub trait Authn {
    async fn get_token(&self) -> Result<String, Error>;
}

pub fn create(auth_name: String, auth_params: String) -> Result<Box<dyn Authn>, Error> {
    match auth_name.to_ascii_lowercase().as_str() {
        "token" => Ok(Box::new(TokenAuthn {
            params: auth_params
        })),
        "oauth2" => Ok(OAuth2Authn::create(auth_params.as_str())?),
        _ => Err(format!("invalid auth [{}], [{}]", auth_name, auth_params).into()),
    }
}

struct TokenAuthn {
    params: String,
}

#[async_trait]
impl Authn for TokenAuthn {
    async fn get_token(&self) -> Result<String, Error> {
        Ok(self.params.clone())
    }
}
