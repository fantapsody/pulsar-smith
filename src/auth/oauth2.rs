use std::fs;

use async_trait::async_trait;
use oauth2::{AuthUrl, ClientId, ClientSecret, TokenResponse, TokenUrl};
use oauth2::AuthType::RequestBody;
use oauth2::basic::BasicClient;
use oauth2::reqwest::async_http_client;
use serde::Deserialize;

use crate::auth::auth::Authn;
use crate::auth::error::Error;

pub(crate) struct OAuth2Authn {
    params: OAuth2Params,
}

#[derive(Deserialize, Debug)]
struct OAuth2Params {
    credentials_url: String,
    issuer_url: String,
    audience: String,
    scope: Option<String>,
}

#[derive(Deserialize, Debug)]
struct OAuth2PrivateParams {
    client_id: String,
    client_secret: String,
    client_email: String,
    issuer_url: String,
}

impl OAuth2Authn {
    pub(crate) fn create(auth_params: &str) -> Result<Box<dyn Authn>, Error> {
        Ok(Box::new(OAuth2Authn {
            params: serde_json::from_str(auth_params)?
        }))
    }
}

impl OAuth2Authn {
    fn read_private_params(&self) -> Result<OAuth2PrivateParams, Error> {
        let fn_name = self.params.credentials_url.strip_prefix("file://")
            .expect("key file should start with file://");
        Ok(serde_json::from_str(fs::read_to_string(fn_name)?.as_str())?)
    }
}

#[async_trait]
impl Authn for OAuth2Authn {
    fn auth_method_name(&self) -> String {
        "token".to_string()
    }

    async fn initialize(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn get_token(&self) -> Result<String, Error> {
        let private_params = self.read_private_params()?;
        let token_url = if private_params.issuer_url.ends_with("/") {
            private_params.issuer_url.clone() + "oauth/token"
        } else {
            private_params.issuer_url.clone() + "/oauth/token"
        };
        let client =
            BasicClient::new(
                ClientId::new(private_params.client_id.clone()),
                Some(ClientSecret::new(private_params.client_secret.clone())),
                AuthUrl::new(private_params.issuer_url.clone())?,
                Some(TokenUrl::new(token_url)?),
            );
        match client
            .set_auth_type(RequestBody)
            .exchange_client_credentials()
            .add_extra_param("audience", self.params.audience.clone())
            .request_async(async_http_client).await {
            Ok(token) => {
                info!("got a oauth2 token for {}", self.params.audience);
                Ok(token.access_token().secret().clone())
            }
            Err(e) => Err(Error::Custom(format!("{:?}", e))),
        }
    }
}
