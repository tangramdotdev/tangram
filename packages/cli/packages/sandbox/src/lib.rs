use {tangram_cli_core::Cli, tangram_client::prelude::*};

#[cfg(target_os = "linux")]
pub mod configure;
pub mod create;
pub mod destroy;
pub mod get;
pub mod init;
pub mod list;
pub mod run;
pub mod serve;
pub mod start;
pub mod status;
pub mod stop;
pub mod wait;

/// Manage sandboxes.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	#[cfg(target_os = "linux")]
	#[usage(hide = true)]
	Configure(self::configure::Args),

	Create(self::create::Args),

	Destroy(self::destroy::Args),

	Get(self::get::Args),

	#[usage(hide = true)]
	Init(self::init::Args),

	#[usage(alias = "ls")]
	List(self::list::Args),

	#[usage(hide = true)]
	Run(self::run::Args),

	#[usage(hide = true)]
	Serve(self::serve::Args),

	Start(self::start::Args),

	Status(self::status::Args),

	Stop(self::stop::Args),

	Wait(self::wait::Args),
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Options {
	#[usage(alias = "cpu-max", long = "cpu")]
	pub cpu: Option<tg::sandbox::Amount>,

	#[usage(long = "cpu-min")]
	pub cpu_min: Option<tg::sandbox::Amount>,

	#[usage(long = "hostname")]
	pub hostname: Option<String>,

	#[usage(alias = "memory-max", long = "memory")]
	pub memory: Option<Bytes>,

	#[usage(long = "memory-min")]
	pub memory_min: Option<Bytes>,

	#[usage(long = "mount", short = 'm')]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[usage(flatten)]
	pub network: Network,

	#[usage(flatten)]
	pub owner: Owner,

	#[usage(long = "port", short = 'p')]
	pub ports: Vec<tg::sandbox::Port>,

	#[usage(alias = "storage-max", long = "storage")]
	pub storage: Option<Bytes>,

	#[usage(long = "storage-min")]
	pub storage_min: Option<Bytes>,
}

#[derive(Clone, Copy, Debug)]
pub struct Bytes(pub u64);

#[derive(Clone, Copy, Debug)]
pub struct Name<T>(pub T);

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Network {
	/// Enable networking.
	#[usage(
		default_missing = "",
		long = "network",
		overrides("--no-network"),
		require_equals = true
	)]
	network: Option<NetworkName>,

	#[usage(long = "no-network", bool_value, overrides("--network"))]
	no_network: bool,
}

#[derive(Clone, Debug)]
struct NetworkName(tg::sandbox::Network);

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Owner {
	#[usage(long = "owner", conflicts("--user", "--group", "--organization"))]
	value: Option<tg::principal::Selector>,

	#[usage(long = "user", conflicts("--owner", "--group", "--organization"))]
	user: Option<tg::principal::Selector>,

	#[usage(long = "group", conflicts("--owner", "--user", "--organization"))]
	group: Option<tg::principal::Selector>,

	#[usage(
		alias = "org",
		long = "organization",
		conflicts("--owner", "--user", "--group")
	)]
	organization: Option<tg::principal::Selector>,
}

#[derive(Clone, Copy, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Http2 {
	#[usage(long = "http2-connection-window-size")]
	connection_window_size: u32,

	#[usage(long = "http2-max-concurrent-reset-streams")]
	max_concurrent_reset_streams: usize,

	#[usage(long = "http2-max-concurrent-streams")]
	max_concurrent_streams: Option<u32>,

	#[usage(long = "http2-stream-window-size")]
	stream_window_size: u32,
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
	let network = network.get();
	if ports.is_empty() {
		return Ok(network);
	}
	match network {
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

#[must_use]
pub fn cpu(options: &Options) -> Option<tg::sandbox::Cpu> {
	if options.cpu.is_none() && options.cpu_min.is_none() {
		return None;
	}
	let min = options.cpu_min.unwrap_or_default();
	let max = options.cpu;
	Some(tg::sandbox::Cpu { max, min })
}

#[must_use]
pub fn resource(min: Option<Bytes>, max: Option<Bytes>) -> Option<tg::sandbox::Resource<u64>> {
	let min = min.map(|min| min.0);
	let max = max.map(|max| max.0);
	if min.is_none() && max.is_none() {
		return None;
	}
	let min = min.unwrap_or_default();
	Some(tg::sandbox::Resource { max, min })
}

pub async fn resolve_creator(
	client: &tg::Client,
	creator: Option<tg::principal::Selector>,
) -> tg::Result<Option<tg::Principal>> {
	let Some(creator) = creator else {
		return Ok(None);
	};
	let creator = resolve_any_owner(client, creator).await?;
	Ok(Some(creator))
}

pub async fn resolve_owner(
	client: &tg::Client,
	owner: &Owner,
) -> tg::Result<Option<tg::Principal>> {
	if let Some(owner) = owner.value.clone() {
		Ok(Some(resolve_any_owner(client, owner).await?))
	} else if let Some(user) = owner.user.clone() {
		Ok(Some(resolve_user_owner(client, user).await?))
	} else if let Some(group) = owner.group.clone() {
		Ok(Some(resolve_group_owner(client, group).await?))
	} else if let Some(organization) = owner.organization.clone() {
		Ok(Some(
			resolve_organization_owner(client, organization).await?,
		))
	} else {
		Ok(None)
	}
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		#[cfg(target_os = "linux")]
		Command::Configure(args) => {
			cli.exit = Some(crate::configure::run(args)?);
		},
		Command::Create(args) => {
			crate::create::run(cli, args).await?;
		},
		Command::Destroy(args) => {
			crate::destroy::run(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::run(cli, args).await?;
		},
		Command::Init(args) => {
			cli.exit = Some(crate::init::run(args)?);
		},
		Command::List(args) => {
			crate::list::run(cli, args).await?;
		},
		Command::Run(args) => {
			cli.exit = Some(crate::run::run(args)?);
		},
		Command::Serve(args) => {
			crate::serve::run(args).await?;
		},
		Command::Start(args) => {
			crate::start::run(cli, args).await?;
		},
		Command::Status(args) => {
			crate::status::run(cli, args).await?;
		},
		Command::Stop(args) => {
			crate::stop::run(cli, args).await?;
		},
		Command::Wait(args) => {
			crate::wait::run(cli, args).await?;
		},
	}
	Ok(())
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
	#[must_use]
	pub fn with_network(network: tg::sandbox::Network) -> Self {
		Self {
			network: Some(NetworkName(network)),
			no_network: false,
		}
	}

	#[must_use]
	pub fn get(&self) -> Option<tg::sandbox::Network> {
		if self.no_network {
			None
		} else {
			self.network.clone().map(|network| network.0)
		}
	}
}

impl Options {
	#[must_use]
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
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.value.is_none()
			&& self.user.is_none()
			&& self.group.is_none()
			&& self.organization.is_none()
	}
}

async fn resolve_any_owner(
	client: &tg::Client,
	owner: tg::principal::Selector,
) -> tg::Result<tg::Principal> {
	match owner {
		tg::principal::Selector::Principal(principal) => resolve_owner_principal(principal),
		tg::principal::Selector::Specifier(specifier) => {
			resolve_any_owner_specifier(client, specifier).await
		},
	}
}

async fn resolve_user_owner(
	client: &tg::Client,
	owner: tg::principal::Selector,
) -> tg::Result<tg::Principal> {
	match owner {
		tg::principal::Selector::Principal(principal) => {
			let principal = resolve_owner_principal(principal)?;
			if !matches!(principal, tg::Principal::User(_)) {
				return Err(tg::error!("the owner is not a user"));
			}
			Ok(principal)
		},
		tg::principal::Selector::Specifier(specifier) => {
			resolve_user_owner_specifier(client, specifier)
				.await?
				.ok_or_else(|| tg::error!("failed to resolve the owner as a user"))
		},
	}
}

async fn resolve_group_owner(
	client: &tg::Client,
	owner: tg::principal::Selector,
) -> tg::Result<tg::Principal> {
	match owner {
		tg::principal::Selector::Principal(principal) => {
			let principal = resolve_owner_principal(principal)?;
			if !matches!(principal, tg::Principal::Group(_)) {
				return Err(tg::error!("the owner is not a group"));
			}
			Ok(principal)
		},
		tg::principal::Selector::Specifier(specifier) => {
			resolve_group_owner_specifier(client, specifier)
				.await?
				.ok_or_else(|| tg::error!("failed to resolve the owner as a group"))
		},
	}
}

async fn resolve_organization_owner(
	client: &tg::Client,
	owner: tg::principal::Selector,
) -> tg::Result<tg::Principal> {
	match owner {
		tg::principal::Selector::Principal(principal) => {
			let principal = resolve_owner_principal(principal)?;
			if !matches!(principal, tg::Principal::Organization(_)) {
				return Err(tg::error!("the owner is not an organization"));
			}
			Ok(principal)
		},
		tg::principal::Selector::Specifier(specifier) => {
			resolve_organization_owner_specifier(client, specifier)
				.await?
				.ok_or_else(|| tg::error!("failed to resolve the owner as an organization"))
		},
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
	client: &tg::Client,
	specifier: tg::Specifier,
) -> tg::Result<tg::Principal> {
	let Some(kind) = try_resolve_owner_specifier_kind(client, specifier.clone()).await? else {
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
	client: &tg::Client,
	specifier: tg::Specifier,
	kind: tg::id::Kind,
) -> tg::Result<Option<tg::Specifier>> {
	let Some(actual) = try_resolve_owner_specifier_kind(client, specifier.clone()).await? else {
		return Ok(None);
	};
	if actual != kind {
		return Ok(None);
	}
	Ok(Some(specifier))
}

async fn resolve_user_owner_specifier(
	client: &tg::Client,
	specifier: tg::Specifier,
) -> tg::Result<Option<tg::Principal>> {
	let Some(specifier) =
		try_resolve_owner_specifier_as_kind(client, specifier, tg::id::Kind::User).await?
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
	client: &tg::Client,
	specifier: tg::Specifier,
) -> tg::Result<Option<tg::Principal>> {
	let Some(specifier) =
		try_resolve_owner_specifier_as_kind(client, specifier, tg::id::Kind::Group).await?
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
	client: &tg::Client,
	specifier: tg::Specifier,
) -> tg::Result<Option<tg::Principal>> {
	let Some(specifier) =
		try_resolve_owner_specifier_as_kind(client, specifier, tg::id::Kind::Organization).await?
	else {
		return Ok(None);
	};
	let selector = tg::organization::Selector::Specifier(specifier);
	let organization = client
		.try_get_organization(&selector, tg::organization::get::Arg::default())
		.await?;
	Ok(organization.map(|organization| tg::Principal::Organization(organization.data.id)))
}

impl<T: serde::de::DeserializeOwned> std::str::FromStr for Name<T> {
	type Err = serde_json::Error;

	fn from_str(value: &str) -> Result<Self, Self::Err> {
		serde_json::from_value(serde_json::Value::String(value.to_owned())).map(Self)
	}
}

impl std::str::FromStr for NetworkName {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self> {
		match s {
			"" | "default" | "true" => Ok(Self(tg::sandbox::Network::Default)),
			"bridge" => Ok(Self(tg::sandbox::Network::Bridge(
				tg::sandbox::Bridge::default(),
			))),
			"host" => Ok(Self(tg::sandbox::Network::Host)),
			_ => Err(tg::error!(%s, "invalid network")),
		}
	}
}

impl std::str::FromStr for Bytes {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self> {
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
		.find_map(|(suffix, multiplier)| {
			value.strip_suffix(suffix).map(|value| (value, multiplier))
		})
		.unwrap_or((value, 1));
		let value = value
			.parse::<u64>()
			.map_err(|error| tg::error!(!error, "invalid resource byte count"))?;
		value
			.checked_mul(multiplier)
			.map(Self)
			.ok_or_else(|| tg::error!("the resource byte count is too large"))
	}
}
