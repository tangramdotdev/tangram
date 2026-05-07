use {crate::Cli, tangram_client::prelude::*};

pub mod login;
pub mod logout;
pub mod whoami;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct User {
	pub id: tg::user::Id,
	pub token: String,
}

/// Manage the user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Login(self::login::Args),
	Logout(self::logout::Args),
	Whoami(self::whoami::Args),
}

impl Cli {
	pub async fn command_user(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Login(args) => {
				self.command_user_login(args).await?;
			},
			Command::Logout(args) => {
				self.command_user_logout(args).await?;
			},
			Command::Whoami(args) => {
				self.command_user_whoami(args).await?;
			},
		}
		Ok(())
	}

	pub(crate) fn user_token(&self) -> Option<String> {
		if let Some(token) = self.args.token.clone() {
			return Some(token);
		}
		if let Ok(token) = std::env::var("TANGRAM_TOKEN") {
			return Some(token);
		}
		self.config
			.as_ref()
			.and_then(|config| config.user.as_ref())
			.map(|user| user.token.clone())
	}

	pub(crate) fn write_user(&mut self, user: User) -> tg::Result<()> {
		let mut config = self.config.clone().unwrap_or_default();
		config.user = Some(user);
		self.write_config(&config)?;
		self.config = Some(config);
		Ok(())
	}

	pub(crate) fn delete_user(&mut self) -> tg::Result<()> {
		if let Some(mut config) = self.config.clone() {
			config.user = None;
			self.write_config(&config)?;
			self.config = Some(config);
		}
		Ok(())
	}
}
