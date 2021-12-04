use async_trait::async_trait;

use crate::context::PulsarContext;
use crate::error::Error;

#[async_trait]
pub trait AsyncCmd: Send + Sync + 'static {
    async fn run(&self, pulsar_ctx: &mut PulsarContext) -> Result<(), Error>;
}
