use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod billing;
pub mod get;
pub mod login;
pub mod logout;
pub mod token;
pub mod usage;
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
	Billing(self::billing::Args),
	Get(self::get::Args),
	Login(self::login::Args),
	Logout(self::logout::Args),
	Token(self::token::Args),
	Usage(self::usage::Args),
	Whoami(self::whoami::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Billing(args) => {
			crate::billing::run(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::run(cli, args).await?;
		},
		Command::Login(args) => {
			crate::login::run(cli, args).await?;
		},
		Command::Logout(args) => {
			crate::logout::run(cli, args).await?;
		},
		Command::Token(args) => {
			crate::token::run(cli, args).await?;
		},
		Command::Usage(args) => {
			crate::usage::run(cli, args).await?;
		},
		Command::Whoami(args) => {
			crate::whoami::run(cli, args).await?;
		},
	}
	Ok(())
}
