use {crate::Cli, tangram_client::prelude::*};

/// Get the current user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_whoami(&mut self, args: Args) -> tg::Result<()> {
		let token = self
			.user_token()
			.ok_or_else(|| tg::error!("not logged in"))?;
		let client = self.client().await?;
		let user = client
			.get_user(&token)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the user"))?
			.ok_or_else(|| tg::error!("not logged in"))?;
		self.print_serde(user, args.print).await?;
		Ok(())
	}
}
