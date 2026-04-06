use {crate::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod init;
pub mod list;

/// Manage sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Create(self::create::Args),
	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),
	#[command(alias = "ls")]
	List(self::list::Args),
	#[command(hide = true)]
	Init(self::init::Args),
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(action = clap::ArgAction::Append, long = "mount", num_args = 1, short)]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[clap(flatten)]
	pub network: Network,

	#[arg(id = "sandbox_ttl", long = "sandbox-ttl")]
	pub ttl: Option<u64>,

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
		self.network
			.or(self.no_network.map(|v| !v))
			.unwrap_or(false)
	}

	pub fn new(network: bool) -> Self {
		Self {
			network: Some(network),
			no_network: None,
		}
	}

	pub fn try_get(&self) -> Option<bool> {
		self.network.or(self.no_network.map(|v| !v))
	}
}

impl Options {
	pub fn into_arg_with_default_ttl(self, ttl: u64) -> tg::sandbox::create::Arg {
		tg::sandbox::create::Arg {
			hostname: self.hostname,
			mounts: self.mounts,
			network: self.network.get(),
			ttl: self.ttl.unwrap_or(ttl),
			user: self.user,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.hostname.is_none()
			&& self.mounts.is_empty()
			&& self.network.try_get().is_none()
			&& self.ttl.is_none()
			&& self.user.is_none()
	}
}

impl Cli {
	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => {
				self.command_sandbox_create(args).await?;
			},
			Command::Delete(args) => {
				self.command_sandbox_delete(args).await?;
			},
			Command::List(args) => {
				self.command_sandbox_list(args).await?;
			},
			Command::Init(_) => {
				unreachable!()
			},
		}
		Ok(())
	}
}
