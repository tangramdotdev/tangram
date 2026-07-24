use {crate::Cli, tangram_client::prelude::*};

/// Log out.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_user_logout(&mut self, _args: Args) -> tg::Result<()> {
		if self.user_token().is_none() {
			return Ok(());
		}
		let client = self.client().await?;
		client
			.logout()
			.await
			.map_err(|error| tg::error!(!error, "failed to log out"))?;
		self.delete_token()?;
		Ok(())
	}
}
