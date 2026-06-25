use {crate::Cli, tangram_client::prelude::*};

#[cfg(target_os = "linux")]
pub mod container;
pub mod create;
pub mod destroy;
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
	Destroy(self::destroy::Args),
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
	#[arg(id = "sandbox.cpu", long = "cpu")]
	pub cpu: Option<u64>,

	#[arg(id = "sandbox.hostname", long = "hostname")]
	pub hostname: Option<String>,

	#[arg(id = "sandbox.isolation", long = "isolation")]
	pub isolation: Option<tg::sandbox::Isolation>,

	#[arg(id = "sandbox.memory", long = "memory")]
	pub memory: Option<u64>,

	#[arg(action = clap::ArgAction::Append, id = "sandbox.mounts", long = "mount", num_args = 1, short = 'm')]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[clap(flatten)]
	pub network: Network,

	#[arg(id = "sandbox.owner", long = "owner")]
	pub owner: Option<tg::principal::Selector>,

	#[arg(action = clap::ArgAction::Append, id = "sandbox.ports", long = "port", num_args = 1, short = 'p')]
	pub ports: Vec<tg::sandbox::Port>,

	#[arg(id = "sandbox.user", long = "user")]
	pub user: Option<String>,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Network {
	/// Enable networking.
	#[arg(
		default_missing_value = "",
		id = "sandbox.network.network",
		long = "network",
		num_args = 0..=1,
		overrides_with = "sandbox.network.no_network",
		require_equals = true,
		value_parser = parse_network,
	)]
	network: Option<tg::sandbox::Network>,

	#[arg(
		default_missing_value = "true",
		id = "sandbox.network.no_network",
		long = "no-network",
		num_args = 0..=1,
		overrides_with = "sandbox.network.network",
		require_equals = true,
	)]
	no_network: bool,
}

impl Network {
	pub fn with_network(network: tg::sandbox::Network) -> Self {
		Self {
			network: Some(network),
			no_network: false,
		}
	}

	pub fn get(&self) -> Option<tg::sandbox::Network> {
		if self.no_network {
			None
		} else {
			self.network.clone()
		}
	}
}

fn parse_network(s: &str) -> tg::Result<tg::sandbox::Network> {
	match s {
		"" | "default" | "true" => Ok(tg::sandbox::Network::Default),
		"bridge" => Ok(tg::sandbox::Network::Bridge(tg::sandbox::Bridge::default())),
		"host" => Ok(tg::sandbox::Network::Host),
		_ => Err(tg::error!(%s, "invalid network")),
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
			&& self.owner.is_none()
			&& self.ports.is_empty()
			&& self.user.is_none()
	}
}

impl Cli {
	pub async fn resolve_owner(
		&self,
		client: &tg::Client,
		owner: Option<tg::principal::Selector>,
	) -> tg::Result<Option<tg::Principal>> {
		let Some(owner) = owner else {
			return Ok(None);
		};
		let owner = match owner {
			tg::principal::Selector::Principal(principal) => match principal {
				tg::grant::Principal::Group(id) => tg::Principal::Group(id),
				tg::grant::Principal::Organization(id) => tg::Principal::Organization(id),
				tg::grant::Principal::Process(id) => tg::Principal::Process(id),
				tg::grant::Principal::Public => {
					return Err(tg::error!("invalid sandbox owner"));
				},
				tg::grant::Principal::Root => tg::Principal::Root,
				tg::grant::Principal::Runner => tg::Principal::Runner,
				tg::grant::Principal::Sandbox(id) => tg::Principal::Sandbox(id),
				tg::grant::Principal::User(id) => tg::Principal::User(id),
			},
			tg::principal::Selector::Specifier(specifier) => {
				let selector = tg::Selector::Specifier(specifier.clone());
				if let Some(group) = client
					.try_get_group(&selector, tg::group::get::Arg::default())
					.await?
				{
					tg::Principal::Group(group.id)
				} else {
					let selector = tg::Selector::Specifier(specifier.clone());
					if let Some(organization) = client
						.try_get_organization(&selector, tg::organization::get::Arg::default())
						.await?
					{
						tg::Principal::Organization(organization.id)
					} else {
						let selector = tg::Selector::Specifier(specifier);
						let Some(user) = client
							.try_get_user(&selector, tg::user::get::Arg::default())
							.await?
						else {
							return Err(tg::error!("failed to resolve the sandbox owner"));
						};
						tg::Principal::User(user.id)
					}
				}
			},
		};
		Ok(Some(owner))
	}

	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			#[cfg(target_os = "linux")]
			Command::Container(args) => {
				self.command_sandbox_container(args).await?;
			},
			Command::Create(args) => {
				self.command_sandbox_create(args).await?;
			},
			Command::Destroy(args) => {
				self.command_sandbox_destroy(args).await?;
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

pub fn normalize_network(
	network: &Network,
	ports: Vec<tg::sandbox::Port>,
) -> tg::Result<Option<tg::sandbox::Network>> {
	if network.no_network {
		return if ports.is_empty() {
			Ok(None)
		} else {
			Err(tg::error!("ports require networking"))
		};
	}
	if ports.is_empty() {
		return Ok(network.network.clone());
	}
	match network.network.clone() {
		Some(tg::sandbox::Network::Host) => {
			Err(tg::error!("ports are not supported with host networking"))
		},
		Some(tg::sandbox::Network::Bridge(mut bridge)) => {
			bridge.ports.extend(ports);
			Ok(Some(tg::sandbox::Network::Bridge(bridge)))
		},
		_ => Ok(Some(tg::sandbox::Network::Bridge(tg::sandbox::Bridge {
			ports,
		}))),
	}
}
