use {crate::Cli, tangram_client::prelude::*, tangram_futures::stream::TryExt as _};

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

	#[arg(id = "sandbox.enqueue", long = "enqueue")]
	pub enqueue: bool,

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

	#[clap(flatten)]
	pub owner: Owner,

	#[arg(action = clap::ArgAction::Append, id = "sandbox.ports", long = "port", num_args = 1, short = 'p')]
	pub ports: Vec<tg::sandbox::Port>,
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

	fn resolve_owner_principal(principal: tg::grant::Principal) -> tg::Result<tg::Principal> {
		let principal = match principal {
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
				let selector = tg::Selector::Specifier(specifier);
				let group = client
					.try_get_group(&selector, tg::group::get::Arg::default())
					.await?
					.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?;
				Ok(tg::Principal::Group(group.id))
			},
			tg::id::Kind::Organization => {
				let selector = tg::Selector::Specifier(specifier);
				let organization = client
					.try_get_organization(&selector, tg::organization::get::Arg::default())
					.await?
					.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?;
				Ok(tg::Principal::Organization(organization.id))
			},
			tg::id::Kind::User => {
				let selector = tg::Selector::Specifier(specifier);
				let user = client
					.try_get_user(&selector, tg::user::get::Arg::default())
					.await?
					.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?;
				Ok(tg::Principal::User(user.id))
			},
			_ => Err(tg::error!("failed to resolve the sandbox owner")),
		}
	}

	async fn try_resolve_owner_specifier_kind(
		&self,
		client: &tg::Client,
		specifier: tg::Specifier,
	) -> tg::Result<Option<tg::id::Kind>> {
		let reference = tg::Reference::with_item(tg::reference::Item::Specifier(specifier.into()));
		let stream = client.try_get(&reference, tg::get::Arg::default()).await?;
		let stream = std::pin::pin!(stream);
		let Some(event) = stream.try_last().await? else {
			return Ok(None);
		};
		let Some(output) = event
			.try_unwrap_output()
			.ok()
			.ok_or_else(|| tg::error!("expected the output"))?
		else {
			return Ok(None);
		};
		let referent = output.referent;
		let id = referent
			.item
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
		let selector = tg::Selector::Specifier(specifier);
		let user = client
			.try_get_user(&selector, tg::user::get::Arg::default())
			.await?;
		Ok(user.map(|user| tg::Principal::User(user.id)))
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
		let selector = tg::Selector::Specifier(specifier);
		let group = client
			.try_get_group(&selector, tg::group::get::Arg::default())
			.await?;
		Ok(group.map(|group| tg::Principal::Group(group.id)))
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
		let selector = tg::Selector::Specifier(specifier);
		let organization = client
			.try_get_organization(&selector, tg::organization::get::Arg::default())
			.await?;
		Ok(organization.map(|organization| tg::Principal::Organization(organization.id)))
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
