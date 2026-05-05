use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

/// Get a reference.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the metadata.
	#[arg(long)]
	pub metadata: bool,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_get(&mut self, args: Args) -> tg::Result<()> {
		let locations = args.locations;
		let print = args.print;
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item() {
			tg::Either::Left(edge) => tg::Either::Left(
				edge.try_unwrap_object_ref()
					.map_err(|_| tg::error!("expected an object"))?
					.id(),
			),
			tg::Either::Right(process) => {
				let id = process
					.id()
					.right()
					.ok_or_else(|| tg::error!("expected a process id"))?
					.clone();
				tg::Either::Right(id)
			},
		};
		let referent = referent.map(|_| item);
		self.print_info_message(&referent.to_string());
		match referent.item {
			tg::Either::Left(object) => {
				let args = crate::object::get::Args {
					bytes: args.bytes,
					locations: locations.clone(),
					metadata: args.metadata,
					object,
					print,
				};
				self.command_object_get(args).await?;
			},
			tg::Either::Right(process) => {
				let args = crate::process::get::Args {
					locations,
					metadata: args.metadata,
					print,
					process,
				};
				self.command_process_get(args).await?;
			},
		}
		Ok(())
	}

	pub(crate) async fn get_reference(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<tg::Referent<tg::Either<tg::graph::Edge<tg::Object>, tg::Process>>> {
		self.get_reference_with_arg(reference, tg::get::Arg::default())
			.boxed()
			.await
	}

	pub(crate) async fn get_reference_with_arg(
		&mut self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<tg::Referent<tg::Either<tg::graph::Edge<tg::Object>, tg::Process>>> {
		if reference.options() == &tg::reference::Options::default() {
			match reference.item() {
				tg::reference::Item::Object(edge) => {
					let edge = match edge {
						tg::graph::data::Edge::Object(id) => {
							tg::graph::Edge::Object(tg::Object::with_id(id.clone()))
						},
						tg::graph::data::Edge::Pointer(pointer) => {
							tg::graph::Edge::Pointer(tg::graph::Pointer {
								graph: pointer.graph.clone().map(tg::Graph::with_id),
								index: pointer.index,
								kind: pointer.kind,
							})
						},
					};
					let referent = tg::Referent::with_item(tg::Either::Left(edge));
					return Ok(referent);
				},
				tg::reference::Item::Process(id) => {
					let process = tg::Process::new(id.clone(), None, None, None, None, None);
					let referent = tg::Referent::with_item(tg::Either::Right(process));
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
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
		}
		let reference = tg::Reference::with_item_and_options(item, options);

		// Get the reference.
		let stream = client
			.get(&reference, arg)
			.await
			.map_err(|source| tg::error!(!source, %reference, "failed to get the reference"))?;
		let mut referent = self
			.render_progress_stream(stream)
			.await
			.map_err(|source| tg::error!(!source, %reference, "failed to get the reference"))?;

		// If the reference is a local relative path, then make the referent's path relative to the current working directory.
		if relative && let Some(path) = referent.path() {
			let current_dir = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
			let path = tangram_util::path::diff(&current_dir, path)
				.map_err(|source| tg::error!(!source, "failed to diff the paths"))?
				.unwrap_or_default();
			referent.options.path = Some(path);
		}

		Ok(referent)
	}

	pub(crate) async fn get_references(
		&mut self,
		references: &[tg::Reference],
	) -> tg::Result<Vec<tg::Referent<tg::Either<tg::graph::Edge<tg::Object>, tg::Process>>>> {
		let mut referents = Vec::with_capacity(references.len());
		for reference in references {
			let referent = self.get_reference(reference).await?;
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
		let referent = self.get_reference(reference).await?;
		let item = referent
			.item
			.clone()
			.left()
			.ok_or_else(|| tg::error!("expected an object"))?;
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
					.map_err(|source| tg::error!(!source, "failed to get the root module"))?;
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
					.map_err(|source| tg::error!(!source, "failed to get the root module"))?;
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
