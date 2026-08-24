use {crate::Cli, futures::FutureExt as _, std::time::Duration, tangram_client::prelude::*};

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
	pub locations: crate::location::Args,

	/// Get the metadata.
	#[arg(long)]
	pub metadata: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,

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

impl Ttl {
	pub(crate) fn get(&self) -> tg::remote::cache::Ttl {
		if self.no_ttl {
			tg::remote::cache::Ttl::Infinite
		} else {
			self.ttl
				.map(tg::remote::cache::Ttl::Duration)
				.unwrap_or_default()
		}
	}
}

impl Cli {
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
			.map(crate::location::Args::with_location)
			.unwrap_or_default();
		let print = args.print;
		self.print_info_message(&referent.to_string());
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
			let options = crate::object::get::Options {
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
			let options = crate::process::get::Options {
				availability: args.availability,
				locations: locations.clone(),
				metadata: args.metadata,
				print,
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
					let tg::user::get::Output {
						emails,
						id,
						location: _,
						name,
						specifier,
						tokens: _,
					} = user;
					let data = tg::user::Data {
						emails,
						id,
						name,
						specifier,
					};
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
					let tg::group::get::Output {
						id,
						location: _,
						name,
						parent,
						specifier,
						tokens: _,
					} = group;
					let data = tg::group::Data {
						id,
						name,
						parent,
						specifier,
					};
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
					let tg::organization::get::Output {
						id,
						location: _,
						name,
						specifier,
						tokens: _,
					} = organization;
					let data = tg::organization::Data {
						id,
						name,
						specifier,
					};
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
					let args = crate::sandbox::get::Args {
						cached: args.cached,
						locations,
						print,
						sandbox: id.try_into()?,
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

	pub(crate) async fn get_with_follow(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::get::Node>> {
		let mut reference = reference.clone();
		let mut options = reference.options().clone();
		options.follow = true;
		reference.set_options(options);
		let output = self.get(&reference).await?;

		Ok(output.referent)
	}

	pub(crate) async fn get_artifact(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::artifact::Id>> {
		let referent = self.get_with_follow(reference).await?;
		let referent = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => id
				.try_into()
				.map_err(|_| tg::error!("expected an artifact")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected an artifact")),
		})?;
		Ok(referent)
	}

	pub(crate) async fn get_object(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::object::Id>> {
		let referent = self.get_with_follow(reference).await?;
		let referent = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected an object")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected an object")),
		})?;
		Ok(referent)
	}

	pub(crate) async fn get_object_with_locations(
		&mut self,
		reference: &tg::Reference,
		locations: &crate::location::Args,
	) -> tg::Result<tg::Referent<tg::object::Id>> {
		let reference = locations.apply_to_reference(reference);
		self.get_object(&reference).await
	}

	pub(crate) async fn get_process(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::process::Id>> {
		let referent = self.get_with_follow(reference).await?;
		let referent = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected a process")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected a process")),
		})?;
		Ok(referent)
	}

	pub(crate) async fn get_process_with_locations(
		&mut self,
		reference: &tg::Reference,
		locations: &crate::location::Args,
	) -> tg::Result<tg::Referent<tg::process::Id>> {
		let reference = locations.apply_to_reference(reference);
		self.get_process(&reference).await
	}

	pub(crate) async fn get(&mut self, reference: &tg::Reference) -> tg::Result<tg::get::Output> {
		self.get_with_arg(reference, tg::get::Arg::default())
			.boxed()
			.await
	}

	pub(crate) async fn get_with_arg(
		&mut self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<tg::get::Output> {
		let tokens = reference.options().tokens.clone();
		let direct_reference =
			tg::Reference::with_node_and_tokens(reference.node().clone(), tokens.clone());
		if reference == &direct_reference {
			match reference.node() {
				tg::reference::Node::Id(id)
					if !tokens.is_empty()
						|| !matches!(
							id.kind(),
							tg::id::Kind::Group
								| tg::id::Kind::Organization
								| tg::id::Kind::Sandbox | tg::id::Kind::Tag
								| tg::id::Kind::User
						) =>
				{
					let referent = tg::Referent::with_node_and_tokens(
						tg::get::Node::Id(id.clone()),
						tokens.clone(),
					);
					let output = tg::get::Output { referent };

					return Ok(output);
				},
				tg::reference::Node::Pointer(pointer) => {
					let referent = tg::Referent::with_node_and_tokens(
						tg::get::Node::Pointer(pointer.clone()),
						tokens,
					);
					let output = tg::get::Output { referent };

					return Ok(output);
				},
				_ => (),
			}
		}

		let client = self.client().await?;

		// Determine if the path is relative.
		let relative = reference
			.node()
			.try_unwrap_path_ref()
			.is_ok_and(|path| path.is_relative());

		// Make the path absolute.
		let mut reference = reference.clone();
		let mut node = reference.node().clone();
		if let tg::reference::Node::Path(path) = &mut node {
			*path = tangram_util::fs::canonicalize_parent(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		}
		reference.set_node(node);

		// Get the reference.
		let stream = client
			.try_get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?;
		let mut output = self
			.render_progress_stream(stream)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?
			.ok_or_else(|| tg::error!(%reference, "failed to get the reference"))?;

		// If the reference is a local relative path, then make the referent's path relative to the current working directory.
		if relative && let Some(path) = output.referent.path() {
			let current_dir = std::env::current_dir()
				.map_err(|error| tg::error!(!error, "failed to get the working directory"))?;
			let path = tangram_util::path::diff(&current_dir, path)
				.map_err(|error| tg::error!(!error, "failed to diff the paths"))?
				.unwrap_or_default();
			output.referent.options.path = Some(path);
		}

		Ok(output)
	}

	pub(crate) async fn get_references(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Referent<tg::get::Node>>> {
		let mut referents = Vec::with_capacity(references.len());
		for reference in references {
			let output = self.get(reference).await?;
			referents.push(output.referent);
		}
		Ok(referents)
	}

	pub(crate) async fn get_modules(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Module>> {
		let mut modules = Vec::with_capacity(references.len());
		for reference in references {
			let module = self.get_module(reference).await?;
			modules.push(module);
		}
		Ok(modules)
	}

	pub(crate) async fn get_module(&mut self, reference: &tg::Reference) -> tg::Result<tg::Module> {
		let client = self.client().await?;

		// Get the reference.
		let referent = self.get_with_follow(reference).await?;
		let mut referent = referent.into_graph_edge()?;
		let module = match referent.node.clone() {
			tg::graph::Edge::Object(tg::Object::Directory(directory)) => {
				let root_module_name = tg::module::try_get_root_module_file_name_with_handle(
					&client,
					tg::Either::Left(&directory),
				)
				.await?
				.ok_or_else(
					|| tg::error!(directory = %directory.id(), "failed to find a root module"),
				)?;
				if let Some(path) = &mut referent.options.path {
					*path = path.join(root_module_name);
				} else {
					referent.options.path.replace(root_module_name.into());
				}
				let kind = tg::module::module_kind_for_path(root_module_name).unwrap();
				let edge = directory
					.get_entry_edge_with_handle(&client, root_module_name)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
				let source = tg::module::Source::Edge(edge.into());
				let referent = referent.map(|_| source);
				tg::Module { kind, referent }
			},

			tg::graph::Edge::Object(tg::Object::File(file)) => {
				let path = referent
					.path()
					.ok_or_else(|| tg::error!("expected a path"))?;
				if !tg::module::is_module_path(path) {
					return Err(tg::error!("expected a module path"));
				}
				let kind = tg::module::module_kind_for_path(path).unwrap();
				let object = file.clone().into();
				let edge = tg::graph::Edge::Object(object);
				let source = tg::module::Source::Edge(edge);
				let referent = referent.map(|_| source);
				tg::Module { kind, referent }
			},

			tg::graph::Edge::Object(tg::Object::Symlink(_)) => {
				return Err(tg::error!("unimplemented"));
			},

			tg::graph::Edge::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Directory => {
				let directory = tg::Directory::with_object(tg::directory::Object::Pointer(pointer));
				let root_module_name = tg::module::try_get_root_module_file_name_with_handle(
					&client,
					tg::Either::Left(&directory),
				)
				.await?
				.ok_or_else(
					|| tg::error!(directory = %directory.id(), "failed to find a root module"),
				)?;
				if let Some(path) = &mut referent.options.path {
					*path = path.join(root_module_name);
				} else {
					referent.options.path.replace(root_module_name.into());
				}
				let kind = tg::module::module_kind_for_path(root_module_name).unwrap();
				let edge = directory
					.get_entry_edge_with_handle(&client, root_module_name)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
				let source = tg::module::Source::Edge(edge.into());
				let referent = referent.map(|_| source);
				tg::Module { kind, referent }
			},

			tg::graph::Edge::Pointer(pointer) if pointer.kind == tg::artifact::Kind::File => {
				let path = referent
					.path()
					.ok_or_else(|| tg::error!("expected a path"))?;
				if !tg::module::is_module_path(path) {
					return Err(tg::error!("expected a module path"));
				}
				let kind = tg::module::module_kind_for_path(path).unwrap();
				let source = tg::module::Source::Edge(tg::graph::Edge::Pointer(pointer.clone()));
				let referent = referent.map(|_| source);
				tg::Module { kind, referent }
			},

			tg::graph::Edge::Pointer(pointer) if pointer.kind == tg::artifact::Kind::Symlink => {
				return Err(tg::error!("unimplemented"));
			},

			_ => {
				return Err(tg::error!("expected an artifact"));
			},
		};

		Ok(module)
	}
}
