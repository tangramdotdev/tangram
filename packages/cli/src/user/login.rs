use {crate::Cli, tangram_client::prelude::*};

/// Log in.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub email: String,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_user_login(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.login_user(tg::user::login::Arg { email: args.email })
			.await
			.map_err(|source| tg::error!(!source, "failed to log in"))?;
		let user = crate::user::User {
			id: output.user.id.clone(),
			token: output.token,
		};
		self.write_user(user)?;
		self.print_serde(output.user, args.print).await?;
		Ok(())
	}
}
