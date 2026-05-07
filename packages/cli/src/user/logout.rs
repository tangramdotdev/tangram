use {crate::Cli, tangram_client::prelude::*};

/// Log out.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_user_logout(&mut self, _args: Args) -> tg::Result<()> {
		self.delete_token()?;
		Ok(())
	}
}
