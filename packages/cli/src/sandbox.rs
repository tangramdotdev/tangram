use {crate::Cli, tangram_client::prelude::*};

// pub mod create;
// pub mod delete;
pub mod run;

/// Manage sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	// Create(self::create::Args),
	// Delete(self::delete::Args),
	Run(self::run::Args),
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
	pub host: Option<String>,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(action = clap::ArgAction::Append, long = "mount", num_args = 1, short)]
	pub mounts: Vec<tg::Either<tg::command::data::Mount, tg::process::Mount>>,

	#[clap(flatten)]
	pub network: Network,

	#[arg(long)]
	pub user: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Network {
	/// Whether to enable the network.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_network",
		require_equals = true,
	)]
	network: Option<bool>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "network",
		require_equals = true,
	)]
	no_network: Option<bool>,
}

impl Network {
	pub fn get(&self) -> bool {
		self.network.or(self.no_network.map(|v| !v)).unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Run(_) => {
				unreachable!()
			},
		}
	}
}
