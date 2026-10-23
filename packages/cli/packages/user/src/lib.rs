use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod billing;
pub mod get;
pub mod login;
pub mod logout;
pub mod token;
pub mod usage;
pub mod whoami;

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Manage the user.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Billing(self::billing::Args),
	Get(self::get::Args),
	Login(self::login::Args),
	Logout(self::logout::Args),
	Token(self::token::Args),
	Usage(self::usage::Args),
	Whoami(self::whoami::Args),
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_user(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Billing(args) => {
				self.command_user_billing(args).await?;
			},
			Command::Get(args) => {
				self.command_user_get(args).await?;
			},
			Command::Login(args) => {
				self.command_user_login(args).await?;
			},
			Command::Logout(args) => {
				self.command_user_logout(args).await?;
			},
			Command::Token(args) => {
				self.command_user_token(args).await?;
			},
			Command::Usage(args) => {
				self.command_user_usage(args).await?;
			},
			Command::Whoami(args) => {
				self.command_user_whoami(args).await?;
			},
		}
		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
