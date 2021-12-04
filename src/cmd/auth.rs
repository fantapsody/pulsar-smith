use async_trait::async_trait;
use clap::Clap;

use crate::cmd::cmd::AsyncCmd;
use crate::context::PulsarContext;
use crate::error::Error;

#[derive(Clap, Debug, Clone)]
pub struct AuthOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[async_trait]
impl AsyncCmd for AuthOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let cmd: &dyn AsyncCmd = match &self.cmd {
            Command::GetToken(opts) => opts,
        };
        cmd.run(pulsar_ctx).await?;
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    GetToken(GetTokenOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct GetTokenOpts {}

#[async_trait]
impl AsyncCmd for GetTokenOpts {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let token = pulsar_ctx.authn()?.get_token().await?;
        println!("{}", token);
        Ok(())
    }
}
