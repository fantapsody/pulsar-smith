use crate::error::Error;

use clap::Clap;

use crate::context::PulsarContext;

#[derive(Clap, Debug, Clone)]
pub struct AuthOpts {
    #[clap(subcommand)]
    pub cmd: Command,
}

impl AuthOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        match &self.cmd {
            Command::GetToken(opts) => opts.run(pulsar_ctx).await?,
        }
        Ok(())
    }
}

#[derive(Clap, Debug, Clone)]
pub enum Command {
    GetToken(GetTokenOpts),
}

#[derive(Clap, Debug, Clone)]
pub struct GetTokenOpts {}

impl GetTokenOpts {
    pub async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error> {
        let token = pulsar_ctx.authn()?.get_token().await?;
        println!("{}", token);
        Ok(())
    }
}
