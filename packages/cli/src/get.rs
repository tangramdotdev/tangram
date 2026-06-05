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

	/// Resolve specifiers to the object or process they select.
	#[arg(long, short = 'R')]
	pub resolve: bool,

	#[command(flatten)]
	pub ttl: Ttl,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Ttl {
	#[arg(long, overrides_with = "no_ttl", value_parser = humantime::parse_duration)]
	pub ttl: Option<Duration>,

	#[arg(long, overrides_with = "ttl")]
	pub no_ttl: bool,
}

impl Ttl {
	fn get(&self) -> Option<Duration> {
		if self.no_ttl { None } else { self.ttl }
	}
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;
		let arg = tg::get::Arg {
			cached: args.cached,
			resolve: args.resolve,
			ttl: args.ttl.get(),
			..Default::default()
		};
		let referent = self.get_reference_with_arg(&args.reference, arg).await?;
		self.print_info_message(&args.reference.to_string());
		match referent.item {
			tg::get::Item::Id(id) => match id.kind() {
				tg::id::Kind::Blob
				| tg::id::Kind::Directory
				| tg::id::Kind::File
				| tg::id::Kind::Symlink
				| tg::id::Kind::Graph
				| tg::id::Kind::Command
				| tg::id::Kind::Error => {
					let args = crate::object::get::Args {
						bytes: args.bytes,
						locations,
						metadata: args.metadata,
						object: id.try_into()?,
						print,
					};
					self.command_object_get(args).await?;
				},
				tg::id::Kind::Process => {
					let args = crate::process::get::Args {
						locations,
						metadata: args.metadata,
						print,
						process: id.try_into()?,
					};
					self.command_process_get(args).await?;
				},
				tg::id::Kind::User => {
					let args = crate::user::get::Args {
						location: locations,
						print,
						user: tg::Selector::Id(id.try_into()?),
					};
					self.command_user_get(args).await?;
				},
				tg::id::Kind::Group => {
					let args = crate::group::get::Args {
						group: tg::Selector::Id(id.try_into()?),
						location: locations,
						print,
					};
					self.command_group_get(args).await?;
				},
				tg::id::Kind::Organization => {
					let args = crate::organization::get::Args {
						location: locations,
						organization: tg::Selector::Id(id.try_into()?),
						print,
					};
					self.command_organization_get(args).await?;
				},
				tg::id::Kind::Tag => {
					let args = crate::tag::get::Args {
						print,
						tag: tg::Selector::Id(id.try_into()?),
					};
					self.command_tag_get(args).await?;
				},
				tg::id::Kind::Sandbox => {
					let args = crate::sandbox::get::Args {
						locations,
						print,
						sandbox: id.try_into()?,
					};
					self.command_sandbox_get(args).await?;
				},
				_ => {
					self.print_serde(id, print).await?;
				},
			},
			tg::get::Item::Pointer(pointer) => {
				self.print_serde(pointer, print).await?;
			},
		}
		Ok(())
	}

	pub(crate) async fn get_reference(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::get::Item>> {
		self.get_reference_with_arg(reference, tg::get::Arg::default())
			.boxed()
			.await
	}

	pub(crate) async fn get_resolved_reference(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::get::Item>> {
		let arg = tg::get::Arg {
			resolve: true,
			..Default::default()
		};
		self.get_reference_with_arg(reference, arg).boxed().await
	}

	pub(crate) async fn get_reference_with_arg(
		&mut self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<tg::Referent<tg::get::Item>> {
		if reference.options() == &tg::reference::Options::default() {
			match reference.item() {
				tg::reference::Item::Id(id) => {
					let referent = tg::Referent::with_item(tg::get::Item::Id(id.clone()));
					return Ok(referent);
				},
				tg::reference::Item::Pointer(pointer) => {
					let referent = tg::Referent::with_item(tg::get::Item::Pointer(pointer.clone()));
					return Ok(referent);
				},
				_ => (),
			}
		}

		let client = self.client().await?;

		// Determine if the path is relative.
		let relative = reference
			.item()
			.try_unwrap_path_ref()
			.is_ok_and(|path| path.is_relative());

		// Make the path absolute.
		let mut item = reference.item().clone();
		let options = reference.options().clone();
		if let tg::reference::Item::Path(path) = &mut item {
			*path = tangram_util::fs::canonicalize_parent(&path)
				.await
				.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;
		}
		let reference = tg::Reference::with_item_and_options(item, options);

		// Get the reference.
		let stream = client
			.get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?;
		let mut referent = self
			.render_progress_stream(stream)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to get the reference"))?;

		// If the reference is a local relative path, then make the referent's path relative to the current working directory.
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

	pub(crate) async fn get_references(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Referent<tg::get::Item>>> {
		let mut referents = Vec::with_capacity(references.len());
		for reference in references {
			let referent = self.get_resolved_reference(reference).await?;
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
		let referent = self.get_resolved_reference(reference).await?;
		let item = referent.item.clone().to_graph_edge()?;
		let mut referent = referent.map(|_| item);
		let module = match referent.item.clone() {
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
				let item = directory
					.get_entry_edge_with_handle(&client, root_module_name)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
				let item = tg::module::Item::Edge(item.into());
				let referent = referent.map(|_| item);
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
				let item = file.clone().into();
				let item = tg::graph::Edge::Object(item);
				let item = tg::module::Item::Edge(item);
				let referent = referent.map(|_| item);
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
				let item = directory
					.get_entry_edge_with_handle(&client, root_module_name)
					.await
					.map_err(|error| tg::error!(!error, "failed to get the root module"))?;
				let item = tg::module::Item::Edge(item.into());
				let referent = referent.map(|_| item);
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
				let item = tg::module::Item::Edge(tg::graph::Edge::Pointer(pointer.clone()));
				let referent = referent.map(|_| item);
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
