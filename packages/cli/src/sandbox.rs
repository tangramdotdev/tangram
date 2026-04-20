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

	#[arg(long)]
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
	pub fn is_empty(&self) -> bool {
		self.cpu.is_none()
			&& self.hostname.is_none()
			&& self.isolation.is_none()
			&& self.memory.is_none()
			&& self.mounts.is_empty()
			&& self.network.try_get().is_none()
			&& self.ttl.is_none()
			&& self.user.is_none()
	}
}

impl Cli {
	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			#[cfg(target_os = "linux")]
			Command::Container(_) => {
				unreachable!()
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
			Command::Serve(_) => {
				unreachable!()
			},
			#[cfg(target_os = "macos")]
			Command::Seatbelt(_) => {
				unreachable!()
			},
			#[cfg(target_os = "linux")]
			Command::Vm(_) => {
				unreachable!()
			},
			Command::List(args) => {
				self.command_sandbox_list(args).await?;
			},
		}
		Ok(())
	}
}
