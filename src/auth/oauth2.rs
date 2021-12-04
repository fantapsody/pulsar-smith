use crate::auth::error::Error;
use std::fs;

use async_trait::async_trait;
use oauth2::{AuthUrl, ClientId, ClientSecret, TokenResponse, TokenUrl};
use oauth2::basic::BasicClient;
use oauth2::reqwest::async_http_client;
use serde::Deserialize;

use crate::auth::auth::Authn;
use oauth2::AuthType::RequestBody;

pub(crate) struct OAuth2Authn {
    params: OAuth2Params,
}

#[derive(Deserialize, Debug)]
struct OAuth2Params {
    #[serde(rename = "privateKey")]
    private_key: String,
    #[serde(rename = "issuerUrl")]
    issuer_url: String,
    #[serde(rename = "audience")]
    audience: String,
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
        let fn_name = self.params.private_key.strip_prefix("file://")
            .expect("key file should start with file://");
        Ok(serde_json::from_str(fs::read_to_string(fn_name)?.as_str())?)
    }
}

#[async_trait]
impl Authn for OAuth2Authn {
    async fn get_token(&self) -> Result<String, Error> {
        let private_params = self.read_private_params()?;
        let client =
            BasicClient::new(
                ClientId::new(private_params.client_id.clone()),
                Some(ClientSecret::new(private_params.client_secret.clone())),
                AuthUrl::new(private_params.issuer_url.clone())?,
                Some(TokenUrl::new(private_params.issuer_url.clone() + "/oauth/token")?),
            );
        match client
            .set_auth_type(RequestBody)
            .exchange_client_credentials()
            .add_extra_param("audience", self.params.audience.clone())
            .request_async(async_http_client).await {
            Ok(token) => Ok(token.access_token().secret().clone()),
            Err(e) => Err(Error::Custom(format!("{:?}", e))),
        }
    }
}
