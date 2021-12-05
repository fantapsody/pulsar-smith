use async_trait::async_trait;

use crate::auth::error::Error;
use crate::auth::oauth2::OAuth2Authn;

#[async_trait]
pub trait Authn: Send + Sync + 'static {
    fn auth_method_name(&self) -> String;

    async fn initialize(&mut self) -> Result<(), Error>;

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
    fn auth_method_name(&self) -> String {
        "auth".to_string()
    }

    async fn initialize(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn get_token(&self) -> Result<String, Error> {
        Ok(self.params.clone())
    }
}
