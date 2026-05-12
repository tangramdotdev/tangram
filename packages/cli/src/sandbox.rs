use {crate::Cli, tangram_client::prelude::*};

#[cfg(target_os = "linux")]
pub mod container;
pub mod create;
pub mod delete;
pub mod get;
pub mod list;
#[cfg(target_os = "macos")]
pub mod seatbelt;
pub mod serve;
#[cfg(target_os = "linux")]
pub mod vm;

/// Manage sandboxes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[cfg(target_os = "linux")]
	#[command(hide = true)]
	Container(self::container::Args),
	Create(self::create::Args),
	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),
	Get(self::get::Args),
	#[command(hide = true)]
	Serve(self::serve::Args),
	#[command(alias = "ls")]
	List(self::list::Args),
	#[cfg(target_os = "macos")]
	#[command(hide = true)]
	Seatbelt(self::seatbelt::Args),
	#[cfg(target_os = "linux")]
	#[command(hide = true)]
	Vm(self::vm::Args),
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[arg(long)]
	pub cpu: Option<u64>,

	#[arg(long)]
	pub hostname: Option<String>,

	#[arg(long)]
	pub isolation: Option<tg::sandbox::Isolation>,

	#[arg(long)]
	pub memory: Option<u64>,

	#[arg(action = clap::ArgAction::Append, long = "mount", num_args = 1, short)]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[clap(flatten)]
	pub network: Network,

	#[arg(action = clap::ArgAction::Append, long = "port", num_args = 1, short = 'p')]
	pub ports: Vec<tg::sandbox::Port>,

	#[arg(long)]
	pub user: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Network {
	/// Enable networking.
	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "no_network",
		require_equals = true,
	)]
	network: Option<tg::Either<bool, tg::sandbox::Network>>,

	#[arg(
		default_missing_value = "true",
		long,
		num_args = 0..=1,
		overrides_with = "network",
		require_equals = true,
	)]
	no_network: bool,
}

impl Network {
	pub fn with_network(network: tg::sandbox::Network) -> Self {
		Self {
			network: Some(tg::Either::Right(network)),
			no_network: false,
		}
	}

	pub fn get(&self) -> Option<tg::Either<bool, tg::sandbox::Network>> {
		if self.no_network {
			Some(tg::Either::Left(false))
		} else {
			self.network.clone()
		}
	}
}

impl Options {
	pub fn is_empty(&self) -> bool {
		self.cpu.is_none()
			&& self.hostname.is_none()
			&& self.isolation.is_none()
			&& self.memory.is_none()
			&& self.mounts.is_empty()
			&& self.network.get().is_none()
			&& self.ports.is_empty()
			&& self.user.is_none()
	}
}

impl Cli {
	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			#[cfg(target_os = "linux")]
			Command::Container(args) => {
				self.command_sandbox_container(args).await?;
			},
			Command::Create(args) => {
				self.command_sandbox_create(args).await?;
			},
			Command::Delete(args) => {
				self.command_sandbox_delete(args).await?;
			},
			Command::Get(args) => {
				self.command_sandbox_get(args).await?;
			},
			Command::Serve(args) => {
				self.command_sandbox_serve(args).await?;
			},
			#[cfg(target_os = "macos")]
			Command::Seatbelt(args) => {
				self.command_sandbox_seatbelt(args).await?;
			},
			#[cfg(target_os = "linux")]
			Command::Vm(args) => {
				self.command_sandbox_vm(args).await?;
			},
			Command::List(args) => {
				self.command_sandbox_list(args).await?;
			},
		}
		Ok(())
	}
}
