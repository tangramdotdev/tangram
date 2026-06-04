use {crate::Cli, tangram_client::prelude::*};

pub mod get;
pub mod grants;
pub mod login;
pub mod logout;
pub mod whoami;

/// Manage the user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Get(self::get::Args),
	Grants(self::grants::Args),
	Login(self::login::Args),
	Logout(self::logout::Args),
	Whoami(self::whoami::Args),
}

impl Cli {
	pub async fn command_user(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Get(args) => {
				self.command_user_get(args).await?;
			},
			Command::Grants(args) => {
				self.command_user_grants(args).await?;
			},
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
		self.config.as_ref().and_then(|config| config.token.clone())
	}

	pub(crate) fn write_token(&mut self, token: String) -> tg::Result<()> {
		let mut config = self.config.clone().unwrap_or_default();
		config.token = Some(token);
		self.write_config(&config)?;
		self.config = Some(config);
		Ok(())
	}

	pub(crate) fn delete_token(&mut self) -> tg::Result<()> {
		if let Some(mut config) = self.config.clone() {
			config.token = None;
			self.write_config(&config)?;
			self.config = Some(config);
		}
		Ok(())
	}
}
