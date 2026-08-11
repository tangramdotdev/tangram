use {crate::Cli, futures::FutureExt as _, std::time::Duration, tangram_client::prelude::*};

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
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

	/// Get the storage status.
	#[arg(long)]
	pub stored: bool,

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
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.reference.options().clone();
		if let Some(location) = args.locations.get() {
			options.location = Some(location);
		}
		let reference =
			tg::Reference::with_node_and_options(args.reference.node().clone(), options);
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
		let locations = args.locations;
		let print = args.print;
		self.print_info_message(&referent.without_token().to_string());
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
			let object = tg::Reference::with_node_and_token(
				tg::reference::Node::Id(object.node.into()),
				object.options.token,
			);
			let args = crate::object::get::Args {
				bytes: args.bytes,
				locations,
				metadata: args.metadata,
				object,
				print,
				stored: args.stored,
			};
			self.command_object_get(args).await?;

			return Ok(());
		}
		if kind == Some(tg::id::Kind::Process) {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let process = tg::Reference::with_node_and_token(
				tg::reference::Node::Id(process.node.into()),
				process.options.token,
			);
			let args = crate::process::get::Args {
				locations,
				metadata: args.metadata,
				print,
				process,
				stored: args.stored,
			};
			self.command_process_get(args).await?;

			return Ok(());
		}
		match referent.node {
			tg::get::Node::Id(id) => match id.kind() {
				tg::id::Kind::User => {
					let args = crate::user::get::Args {
						cached: args.cached,
						location: locations,
						print,
						ttl: args.ttl,
						user: tg::Selector::Id(id.try_into()?),
					};
					self.command_user_get(args).await?;
				},
				tg::id::Kind::Group => {
					let args = crate::group::get::Args {
						cached: args.cached,
						group: tg::Selector::Id(id.try_into()?),
						location: locations,
						print,
						ttl: args.ttl,
					};
					self.command_group_get(args).await?;
				},
				tg::id::Kind::Organization => {
					let args = crate::organization::get::Args {
						cached: args.cached,
						location: locations,
						organization: tg::Selector::Id(id.try_into()?),
						print,
						ttl: args.ttl,
					};
					self.command_organization_get(args).await?;
				},
				tg::id::Kind::Tag => {
					let args = crate::tag::get::Args {
						cached: args.cached,
						location: locations,
						print,
						tag: tg::Selector::Id(id.try_into()?),
						ttl: args.ttl,
					};
					self.command_tag_get(args).await?;
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

	pub(crate) async fn resolve(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::get::Node>> {
		self.resolve_with_arg(reference, tg::resolve::Arg::default())
			.boxed()
			.await
	}

	pub(crate) async fn resolve_artifact(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::artifact::Id>> {
		let referent = self.resolve(reference).await?;
		let referent = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => id
				.try_into()
				.map_err(|_| tg::error!("expected an artifact")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected an artifact")),
		})?;
		Ok(referent)
	}

	pub(crate) async fn resolve_object(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::object::Id>> {
		let referent = self.resolve(reference).await?;
		let referent = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected an object")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected an object")),
		})?;
		Ok(referent)
	}

	pub(crate) async fn resolve_process(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::process::Id>> {
		let referent = self.resolve(reference).await?;
		let referent = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => id.try_into().map_err(|_| tg::error!("expected a process")),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected a process")),
		})?;
		Ok(referent)
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
		let token = reference.options().token.clone();
		let direct_reference =
			tg::Reference::with_node_and_token(reference.node().clone(), token.clone());
		if reference == &direct_reference {
			match reference.node() {
				tg::reference::Node::Id(id)
					if token.is_some()
						|| !matches!(
							id.kind(),
							tg::id::Kind::Group
								| tg::id::Kind::Organization
								| tg::id::Kind::Sandbox | tg::id::Kind::Tag
								| tg::id::Kind::User
						) =>
				{
					let referent = tg::Referent::with_node_and_token(
						tg::get::Node::Id(id.clone()),
						token.clone(),
					);
					let output = tg::get::Output {
						location: None,
						referent,
					};

					return Ok(output);
				},
				tg::reference::Node::Pointer(pointer) => {
					let referent = tg::Referent::with_node_and_token(
						tg::get::Node::Pointer(pointer.clone()),
						token,
					);
					let output = tg::get::Output {
						location: None,
						referent,
					};

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
		let mut node = reference.node().clone();
		let options = reference.options().clone();
		if let tg::reference::Node::Path(path) = &mut node {
			*path = tangram_util::fs::canonicalize_parent(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		}
		let reference = tg::Reference::with_node_and_options(node, options);

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

	pub(crate) async fn resolve_with_arg(
		&mut self,
		reference: &tg::Reference,
		arg: tg::resolve::Arg,
	) -> tg::Result<tg::Referent<tg::resolve::Node>> {
		let token = reference.options().token.clone();
		let direct_reference =
			tg::Reference::with_node_and_token(reference.node().clone(), token.clone());
		if reference == &direct_reference {
			match reference.node() {
				tg::reference::Node::Id(id)
					if token.is_some()
						|| !matches!(
							id.kind(),
							tg::id::Kind::Group
								| tg::id::Kind::Organization
								| tg::id::Kind::Sandbox | tg::id::Kind::Tag
								| tg::id::Kind::User
						) =>
				{
					let referent = tg::Referent::with_node_and_token(
						tg::resolve::Node::Id(id.clone()),
						token.clone(),
					);

					return Ok(referent);
				},
				tg::reference::Node::Pointer(pointer) => {
					let referent = tg::Referent::with_node_and_token(
						tg::resolve::Node::Pointer(pointer.clone()),
						token,
					);

					return Ok(referent);
				},
				_ => (),
			}
		}

		let client = self.client().await?;
		let relative = reference
			.node()
			.try_unwrap_path_ref()
			.is_ok_and(|path| path.is_relative());
		let mut node = reference.node().clone();
		let options = reference.options().clone();
		if let tg::reference::Node::Path(path) = &mut node {
			*path = tangram_util::fs::canonicalize_parent(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		}
		let reference = tg::Reference::with_node_and_options(node, options);
		let stream = client
			.resolve(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to resolve the reference"))?;
		let mut referent = self
			.render_progress_stream(stream)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to resolve the reference"))?;
		if relative && let Some(path) = referent.path() {
			let current_dir = std::env::current_dir()
				.map_err(|error| tg::error!(!error, "failed to get the working directory"))?;
			let path = tangram_util::path::diff(&current_dir, path)
				.map_err(|error| tg::error!(!error, "failed to diff the paths"))?
				.unwrap_or_default();
			referent.options.path = Some(path);
		}

		Ok(referent)
	}

	pub(crate) async fn resolve_references(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Referent<tg::get::Node>>> {
		let mut referents = Vec::with_capacity(references.len());
		for reference in references {
			let referent = self.resolve(reference).await?;
			referents.push(referent);
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
		let referent = self.resolve(reference).await?;
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
