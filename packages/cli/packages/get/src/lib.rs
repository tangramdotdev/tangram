use {std::time::Duration, tangram_cli_core::Cli, tangram_client::prelude::*};

mod ext;

pub use self::ext::Ext;

pub mod object;
pub mod process;
pub mod sandbox;

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the availability.
	#[arg(long)]
	pub availability: bool,

	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	/// Get the metadata.
	#[arg(long)]
	pub metadata: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// Select the source of process or sandbox state.
	#[arg(long, default_value = "auto", value_parser = tangram_cli_core::source_parser())]
	pub source: tg::process::Source,

	#[command(flatten)]
	pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ttl {
	#[arg(id = "get.ttl.ttl", long = "ttl", overrides_with = "get.ttl.no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(id = "get.ttl.no_ttl", long = "no-ttl", overrides_with = "get.ttl.ttl")]
	pub no_ttl: bool,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_get(&mut self, mut args: Args) -> tg::Result<()> {
		args.locations.set_from_reference_if_unset(&args.reference);
		let reference = args.locations.apply_to_reference(&args.reference);
		let arg = tg::get::Arg {
			cached: args.cached,
			ttl: args.ttl.get(),
			..Default::default()
		};
		let referent = self.get_with_arg(&reference, arg).await?.referent;

		self.print_get_output(args, referent).await
	}

	pub(crate) async fn print_get_output(
		&mut self,
		args: Args,
		referent: tg::Referent<tg::get::Node>,
	) -> tg::Result<()> {
		let tokens = args.reference.options().tokens.clone();
		let locations = args
			.locations
			.get_for_options(&referent)
			.map(tangram_cli_core::location::Args::with_location)
			.unwrap_or_default();
		let print = args.print;
		self.print_info_referent(&referent, &print);
		let kind = match referent.node() {
			tg::get::Node::Id(id) => Some(id.kind()),
			tg::get::Node::Pointer(_) => None,
		};
		if kind.is_some_and(|kind| {
			matches!(
				kind,
				tg::id::Kind::Blob
					| tg::id::Kind::Directory
					| tg::id::Kind::File
					| tg::id::Kind::Symlink
					| tg::id::Kind::Graph
					| tg::id::Kind::Command
					| tg::id::Kind::Error
			)
		}) {
			let object = referent.try_map::<tg::object::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let options = crate::object::Options {
				availability: args.availability,
				bytes: args.bytes,
				locations: locations.clone(),
				metadata: args.metadata,
				print,
			};
			self.command_object_get_inner(object, options).await?;

			return Ok(());
		}
		if kind == Some(tg::id::Kind::Process) {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let options = crate::process::Options {
				availability: args.availability,
				locations: locations.clone(),
				metadata: args.metadata,
				print,
				source: args.source,
			};
			self.command_process_get_inner(process, options).await?;

			return Ok(());
		}
		match referent.node {
			tg::get::Node::Id(id) => match id.kind() {
				tg::id::Kind::User => {
					let client = self.client().await?;
					let arg = tg::user::get::Arg {
						cached: args.cached,
						location: locations.get(),
						tokens,
						ttl: args.ttl.get(),
					};
					let user = tg::user::Selector::Id(id.try_into()?);
					let user = client
						.try_get_user(&user, arg)
						.await
						.map_err(
							|error| tg::error!(!error, user = %user, "failed to get the user"),
						)?
						.ok_or_else(|| tg::error!(user = %user, "failed to find the user"))?;
					let data = user.data;
					self.print_serde(data, print).await?;
				},
				tg::id::Kind::Group => {
					let client = self.client().await?;
					let arg = tg::group::get::Arg {
						cached: args.cached,
						location: locations.get(),
						tokens,
						ttl: args.ttl.get(),
					};
					let group = tg::group::Selector::Id(id.try_into()?);
					let group = client
						.try_get_group(&group, arg)
						.await
						.map_err(
							|error| tg::error!(!error, group = %group, "failed to get the group"),
						)?
						.ok_or_else(|| tg::error!(group = %group, "failed to find the group"))?;
					let data = group.data;
					self.print_serde(data, print).await?;
				},
				tg::id::Kind::Organization => {
					let client = self.client().await?;
					let arg = tg::organization::get::Arg {
						cached: args.cached,
						location: locations.get(),
						tokens,
						ttl: args.ttl.get(),
					};
					let organization = tg::organization::Selector::Id(id.try_into()?);
					let organization = client
						.try_get_organization(&organization, arg)
						.await
						.map_err(
							|error| tg::error!(!error, organization = %organization, "failed to get the organization"),
						)?
						.ok_or_else(
							|| tg::error!(organization = %organization, "failed to find the organization"),
						)?;
					let data = organization.data;
					self.print_serde(data, print).await?;
				},
				tg::id::Kind::Tag => {
					let client = self.client().await?;
					let arg = tg::tag::get::Arg {
						cached: args.cached,
						location: locations.get(),
						tokens,
						ttl: args.ttl.get(),
					};
					let tag = tg::tag::Selector::Id(id.try_into()?);
					let tag = client
						.try_get_tag(&tag, arg)
						.await
						.map_err(|error| tg::error!(!error, tag = %tag, "failed to get the tag"))?
						.ok_or_else(|| tg::error!(tag = %tag, "failed to find the tag"))?;
					let tg::tag::get::Output {
						data,
						location: _,
						tokens: _,
					} = tag;
					self.print_serde(data, print).await?;
				},
				tg::id::Kind::Sandbox => {
					let options = tg::referent::Options {
						tokens,
						..Default::default()
					};
					let sandbox = tg::Referent::new(id.try_into()?, options);
					let args = crate::sandbox::Args {
						cached: args.cached,
						locations,
						print,
						sandbox,
						source: args.source,
						ttl: args.ttl,
					};
					self.command_sandbox_get(args).await?;
				},
				_ => {
					self.print_serde(id, print).await?;
				},
			},
			tg::get::Node::Pointer(pointer) => {
				self.print_serde(pointer, print).await?;
			},
		}
		Ok(())
	}
}

impl Ttl {
	pub fn get(&self) -> tg::remote::cache::Ttl {
		if self.no_ttl {
			tg::remote::cache::Ttl::Infinite
		} else {
			self.ttl
				.map(tg::remote::cache::Ttl::Duration)
				.unwrap_or_default()
		}
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
