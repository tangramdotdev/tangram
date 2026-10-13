use {crate::Cli, tangram_client::prelude::*};

pub mod create;
pub mod destroy;
pub mod get;
pub mod init;
pub mod list;
pub mod run;
pub mod serve;
pub mod wait;

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
	Destroy(self::destroy::Args),
	Get(self::get::Args),
	#[command(hide = true)]
	Init(self::init::Args),
	#[command(alias = "ls")]
	List(self::list::Args),
	#[command(hide = true)]
	Run(self::run::Args),
	#[command(hide = true)]
	Serve(self::serve::Args),
	Wait(self::wait::Args),
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[arg(alias = "cpu-max", id = "sandbox.cpu", long = "cpu")]
	pub cpu: Option<tg::sandbox::Amount>,

	#[arg(id = "sandbox.cpu_min", long = "cpu-min")]
	pub cpu_min: Option<tg::sandbox::Amount>,

	#[arg(id = "sandbox.hostname", long = "hostname")]
	pub hostname: Option<String>,

	#[arg(alias = "memory-max", id = "sandbox.memory", long = "memory", value_parser = parse_bytes)]
	pub memory: Option<u64>,

	#[arg(id = "sandbox.memory_min", long = "memory-min", value_parser = parse_bytes)]
	pub memory_min: Option<u64>,

	#[arg(action = clap::ArgAction::Append, id = "sandbox.mounts", long = "mount", num_args = 1, short = 'm')]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[clap(flatten)]
	pub network: Network,

	#[clap(flatten)]
	pub owner: Owner,

	#[arg(action = clap::ArgAction::Append, id = "sandbox.ports", long = "port", num_args = 1, short = 'p')]
	pub ports: Vec<tg::sandbox::Port>,
	#[arg(alias = "storage-max", id = "sandbox.storage", long = "storage", value_parser = parse_bytes)]
	pub storage: Option<u64>,
	#[arg(id = "sandbox.storage_min", long = "storage-min", value_parser = parse_bytes)]
	pub storage_min: Option<u64>,
	#[arg(alias = "swap-max", id = "sandbox.swap", long = "swap", value_parser = parse_bytes)]
	pub swap: Option<u64>,
	#[arg(id = "sandbox.swap_min", long = "swap-min", value_parser = parse_bytes)]
	pub swap_min: Option<u64>,
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

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Owner {
	#[arg(id = "sandbox.owner.value", long = "owner", conflicts_with_all = ["sandbox.owner.user", "sandbox.owner.group", "sandbox.owner.organization"])]
	value: Option<tg::principal::Selector>,

	#[arg(id = "sandbox.owner.user", long = "user", conflicts_with_all = ["sandbox.owner.value", "sandbox.owner.group", "sandbox.owner.organization"])]
	user: Option<tg::principal::Selector>,

	#[arg(id = "sandbox.owner.group", long = "group", conflicts_with_all = ["sandbox.owner.value", "sandbox.owner.user", "sandbox.owner.organization"])]
	group: Option<tg::principal::Selector>,

	#[arg(alias = "org", id = "sandbox.owner.organization", long = "organization", conflicts_with_all = ["sandbox.owner.value", "sandbox.owner.user", "sandbox.owner.group"])]
	organization: Option<tg::principal::Selector>,
}

#[derive(Clone, Copy, Debug, clap::Args)]
pub struct Http2 {
	#[arg(
		id = "sandbox.http2.connection_window_size",
		long = "http2-connection-window-size"
	)]
	connection_window_size: u32,

	#[arg(
		id = "sandbox.http2.max_concurrent_reset_streams",
		long = "http2-max-concurrent-reset-streams"
	)]
	max_concurrent_reset_streams: usize,

	#[arg(
		id = "sandbox.http2.max_concurrent_streams",
		long = "http2-max-concurrent-streams"
	)]
	max_concurrent_streams: Option<u32>,

	#[arg(
		id = "sandbox.http2.stream_window_size",
		long = "http2-stream-window-size"
	)]
	stream_window_size: u32,
}

impl Http2 {
	#[must_use]
	pub fn config(&self) -> tangram_http::http2::Config {
		tangram_http::http2::Config {
			connection_window_size: self.connection_window_size,
			max_concurrent_reset_streams: self.max_concurrent_reset_streams,
			max_concurrent_streams: self.max_concurrent_streams,
			stream_window_size: self.stream_window_size,
		}
	}
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
			&& self.memory.is_none()
			&& self.mounts.is_empty()
			&& self.network.get().is_none()
			&& self.owner.is_empty()
			&& self.ports.is_empty()
	}
}

impl Owner {
	pub fn is_empty(&self) -> bool {
		self.value.is_none()
			&& self.user.is_none()
			&& self.group.is_none()
			&& self.organization.is_none()
	}
}

impl Cli {
	pub async fn resolve_owner(
		&self,
		client: &tg::Client,
		owner: &Owner,
	) -> tg::Result<Option<tg::Principal>> {
		if let Some(owner) = owner.value.clone() {
			Ok(Some(self.resolve_any_owner(client, owner).await?))
		} else if let Some(user) = owner.user.clone() {
			Ok(Some(self.resolve_user_owner(client, user).await?))
		} else if let Some(group) = owner.group.clone() {
			Ok(Some(self.resolve_group_owner(client, group).await?))
		} else if let Some(organization) = owner.organization.clone() {
			Ok(Some(
				self.resolve_organization_owner(client, organization)
					.await?,
			))
		} else {
			Ok(None)
		}
	}

	async fn resolve_any_owner(
		&self,
		client: &tg::Client,
		owner: tg::principal::Selector,
	) -> tg::Result<tg::Principal> {
		match owner {
			tg::principal::Selector::Principal(principal) => {
				Self::resolve_owner_principal(principal)
			},
			tg::principal::Selector::Specifier(specifier) => {
				self.resolve_any_owner_specifier(client, specifier).await
			},
		}
	}

	async fn resolve_user_owner(
		&self,
		client: &tg::Client,
		owner: tg::principal::Selector,
	) -> tg::Result<tg::Principal> {
		match owner {
			tg::principal::Selector::Principal(principal) => {
				let principal = Self::resolve_owner_principal(principal)?;
				if !matches!(principal, tg::Principal::User(_)) {
					return Err(tg::error!("the owner is not a user"));
				}
				Ok(principal)
			},
			tg::principal::Selector::Specifier(specifier) => self
				.resolve_user_owner_specifier(client, specifier)
				.await?
				.ok_or_else(|| tg::error!("failed to resolve the owner as a user")),
		}
	}

	async fn resolve_group_owner(
		&self,
		client: &tg::Client,
		owner: tg::principal::Selector,
	) -> tg::Result<tg::Principal> {
		match owner {
			tg::principal::Selector::Principal(principal) => {
				let principal = Self::resolve_owner_principal(principal)?;
				if !matches!(principal, tg::Principal::Group(_)) {
					return Err(tg::error!("the owner is not a group"));
				}
				Ok(principal)
			},
			tg::principal::Selector::Specifier(specifier) => self
				.resolve_group_owner_specifier(client, specifier)
				.await?
				.ok_or_else(|| tg::error!("failed to resolve the owner as a group")),
		}
	}

	async fn resolve_organization_owner(
		&self,
		client: &tg::Client,
		owner: tg::principal::Selector,
	) -> tg::Result<tg::Principal> {
		match owner {
			tg::principal::Selector::Principal(principal) => {
				let principal = Self::resolve_owner_principal(principal)?;
				if !matches!(principal, tg::Principal::Organization(_)) {
					return Err(tg::error!("the owner is not an organization"));
				}
				Ok(principal)
			},
			tg::principal::Selector::Specifier(specifier) => self
				.resolve_organization_owner_specifier(client, specifier)
				.await?
				.ok_or_else(|| tg::error!("failed to resolve the owner as an organization")),
		}
	}

	fn resolve_owner_principal(principal: tg::Principal) -> tg::Result<tg::Principal> {
		let principal = match principal {
			tg::Principal::Group(id) => tg::Principal::Group(id),
			tg::Principal::Organization(id) => tg::Principal::Organization(id),
			tg::Principal::Process(id) => tg::Principal::Process(id),
			tg::Principal::Anonymous => {
				return Err(tg::error!("invalid sandbox owner"));
			},
			tg::Principal::Root => tg::Principal::Root,
			tg::Principal::Runner(id) => tg::Principal::Runner(id),
			tg::Principal::Sandbox(id) => tg::Principal::Sandbox(id),
			tg::Principal::User(id) => tg::Principal::User(id),
		};
		Ok(principal)
	}

	async fn resolve_any_owner_specifier(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
	) -> tg::Result<tg::Principal> {
		let Some(kind) = self
			.try_resolve_owner_specifier_kind(client, specifier.clone())
			.await?
		else {
			return Err(tg::error!("failed to resolve the sandbox owner"));
		};
		match kind {
			tg::id::Kind::Group => {
				let selector = tg::group::Selector::Specifier(specifier);
				let group = client
					.try_get_group(&selector, tg::group::get::Arg::default())
					.await?
					.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?;
				Ok(tg::Principal::Group(group.data.id))
			},
			tg::id::Kind::Organization => {
				let selector = tg::organization::Selector::Specifier(specifier);
				let organization = client
					.try_get_organization(&selector, tg::organization::get::Arg::default())
					.await?
					.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?;
				Ok(tg::Principal::Organization(organization.data.id))
			},
			tg::id::Kind::User => {
				let selector = tg::user::Selector::Specifier(specifier);
				let user = client
					.try_get_user(&selector, tg::user::get::Arg::default())
					.await?
					.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?;
				Ok(tg::Principal::User(user.data.id))
			},
			_ => Err(tg::error!("failed to resolve the sandbox owner")),
		}
	}

	async fn try_resolve_owner_specifier_kind(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
	) -> tg::Result<Option<tg::id::Kind>> {
		let reference = tg::Reference::with_node(tg::reference::Node::Specifier(specifier.into()));
		let Some(referent) = reference.try_get_with_instance(client).await? else {
			return Ok(None);
		};
		let id = referent
			.node
			.try_unwrap_id()
			.map_err(|_| tg::error!("failed to resolve the sandbox owner"))?;
		Ok(Some(id.kind()))
	}

	async fn try_resolve_owner_specifier_as_kind(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
		kind: tg::id::Kind,
	) -> tg::Result<Option<tg::Specifier>> {
		let Some(actual) = self
			.try_resolve_owner_specifier_kind(client, specifier.clone())
			.await?
		else {
			return Ok(None);
		};
		if actual != kind {
			return Ok(None);
		}
		Ok(Some(specifier))
	}

	async fn resolve_user_owner_specifier(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
	) -> tg::Result<Option<tg::Principal>> {
		let Some(specifier) = self
			.try_resolve_owner_specifier_as_kind(client, specifier, tg::id::Kind::User)
			.await?
		else {
			return Ok(None);
		};
		let selector = tg::user::Selector::Specifier(specifier);
		let user = client
			.try_get_user(&selector, tg::user::get::Arg::default())
			.await?;
		Ok(user.map(|user| tg::Principal::User(user.data.id)))
	}

	async fn resolve_group_owner_specifier(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
	) -> tg::Result<Option<tg::Principal>> {
		let Some(specifier) = self
			.try_resolve_owner_specifier_as_kind(client, specifier, tg::id::Kind::Group)
			.await?
		else {
			return Ok(None);
		};
		let selector = tg::group::Selector::Specifier(specifier);
		let group = client
			.try_get_group(&selector, tg::group::get::Arg::default())
			.await?;
		Ok(group.map(|group| tg::Principal::Group(group.data.id)))
	}

	async fn resolve_organization_owner_specifier(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
	) -> tg::Result<Option<tg::Principal>> {
		let Some(specifier) = self
			.try_resolve_owner_specifier_as_kind(client, specifier, tg::id::Kind::Organization)
			.await?
		else {
			return Ok(None);
		};
		let selector = tg::organization::Selector::Specifier(specifier);
		let organization = client
			.try_get_organization(&selector, tg::organization::get::Arg::default())
			.await?;
		Ok(organization.map(|organization| tg::Principal::Organization(organization.data.id)))
	}

	pub async fn command_sandbox(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => {
				self.command_sandbox_create(args).await?;
			},
			Command::Destroy(args) => {
				self.command_sandbox_destroy(args).await?;
			},
			Command::Get(args) => {
				self.command_sandbox_get(args).await?;
			},
			Command::Init(args) => {
				self.exit = Some(Self::command_sandbox_init(args)?);
			},
			Command::List(args) => {
				self.command_sandbox_list(args).await?;
			},
			Command::Run(args) => {
				self.exit = Some(Self::command_sandbox_run(args)?);
			},
			Command::Serve(args) => {
				self.command_sandbox_serve(args).await?;
			},
			Command::Wait(args) => {
				self.command_sandbox_wait(args).await?;
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

pub(crate) fn cpu(options: &Options) -> Option<tg::sandbox::Cpu> {
	if options.cpu.is_none() && options.cpu_min.is_none() {
		return None;
	}
	let min = options.cpu_min.unwrap_or_default();
	let max = options.cpu.unwrap_or(min);
	Some(tg::sandbox::Cpu { max, min })
}

pub(crate) fn resource(min: Option<u64>, max: Option<u64>) -> Option<tg::sandbox::Resource<u64>> {
	if min.is_none() && max.is_none() {
		return None;
	}
	let min = min.unwrap_or_default();
	let max = max.unwrap_or(min);
	Some(tg::sandbox::Resource { max, min })
}

fn parse_bytes(value: &str) -> tg::Result<u64> {
	let (value, multiplier) = [
		("TiB", 1_u64 << 40),
		("GiB", 1_u64 << 30),
		("MiB", 1_u64 << 20),
		("KiB", 1_u64 << 10),
		("GB", 1_000_000_000),
		("MB", 1_000_000),
		("KB", 1_000),
		("B", 1),
	]
	.into_iter()
	.find_map(|(suffix, multiplier)| value.strip_suffix(suffix).map(|value| (value, multiplier)))
	.unwrap_or((value, 1));
	let value = value
		.parse::<u64>()
		.map_err(|error| tg::error!(!error, "invalid resource byte count"))?;
	value
		.checked_mul(multiplier)
		.ok_or_else(|| tg::error!("the resource byte count is too large"))
}
