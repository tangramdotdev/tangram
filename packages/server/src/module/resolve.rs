use {
	crate::{Server, context::Context},
	std::{path::Path, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn resolve_module_with_context(
		&self,
		_context: &Context,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		let tg::module::resolve::Arg { referrer, import } = arg;

		// Get the referent.
		let referent = match referrer.referent.item() {
			tg::module::data::Item::Edge(edge) => {
				let referrer = referrer.referent.clone().map(|_| edge);
				self.resolve_module_with_edge_referrer(&referrer, &import)
					.await?
			},
			tg::module::data::Item::Path(path) => {
				let referrer = referrer.referent.clone().map(|_| path.as_ref());
				self.resolve_module_with_path_referrer(&referrer, &import)
					.await?
			},
		};

		// If the kind is not known, then try to infer it from the path extension.
		let kind = if let Some(kind) = import.kind {
			Some(kind)
		} else if let Some(path) = referent.path() {
			let extension = path.extension();
			if extension.is_some_and(|extension| extension == "js") {
				Some(tg::module::Kind::Js)
			} else if extension.is_some_and(|extension| extension == "ts") {
				Some(tg::module::Kind::Ts)
			} else {
				None
			}
		} else if let tg::module::data::Item::Path(path) = referent.item() {
			let extension = path.extension();
			if extension.is_some_and(|extension| extension == "js") {
				Some(tg::module::Kind::Js)
			} else if extension.is_some_and(|extension| extension == "ts") {
				Some(tg::module::Kind::Ts)
			} else {
				None
			}
		} else {
			None
		};

		// If the kind is still not known, then infer it from the object's kind.
		let kind = if let Some(kind) = kind {
			kind
		} else {
			match referent.item() {
				tg::module::data::Item::Edge(edge) => match edge.kind() {
					tg::object::Kind::Blob => tg::module::Kind::Blob,
					tg::object::Kind::Directory => tg::module::Kind::Directory,
					tg::object::Kind::File => tg::module::Kind::File,
					tg::object::Kind::Symlink => tg::module::Kind::Symlink,
					tg::object::Kind::Graph => tg::module::Kind::Graph,
					tg::object::Kind::Command => tg::module::Kind::Command,
					tg::object::Kind::Error => tg::module::Kind::Error,
				},

				tg::module::data::Item::Path(path) => {
					let metadata = tokio::fs::symlink_metadata(&path)
						.await
						.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
					if metadata.is_dir() {
						tg::module::Kind::Directory
					} else if metadata.is_file() {
						tg::module::Kind::File
					} else if metadata.is_symlink() {
						tg::module::Kind::Symlink
					} else {
						return Err(tg::error!("expected a directory, file, or symlink"));
					}
				},
			}
		};

		// Create the module.
		let module = tg::module::Data { kind, referent };

		// Create the output.
		let output = tg::module::resolve::Output { module };

		Ok(output)
	}

	async fn resolve_module_with_edge_referrer(
		&self,
		referrer: &tg::Referent<&tg::graph::data::Edge<tg::object::Id>>,
		import: &tg::module::Import,
	) -> tg::Result<tg::Referent<tg::module::data::Item>> {
		let edge = tg::graph::Edge::<tg::Artifact>::try_from_data(referrer.item.clone())?;
		let file = tg::Artifact::with_edge(edge)
			.clone()
			.try_unwrap_file()
			.ok()
			.ok_or_else(|| tg::error!(referrer = %referrer.item, "the referrer must be a file"))?;
		let dependency = file.get_dependency_edge(self, &import.reference).await?;

		let object = match &dependency.0.item {
			Some(tg::graph::Edge::Reference(reference)) => {
				tg::Artifact::with_reference(reference.clone()).into()
			},
			Some(tg::graph::Edge::Object(object)) => object.clone(),
			None => return Err(tg::error!("dependency has no resolved item")),
		};

		let referent = match (import.kind, &object) {
			(
				None | Some(tg::module::Kind::Js | tg::module::Kind::Ts),
				tg::Object::Directory(directory),
			) => {
				let path =
					tg::package::try_get_root_module_file_name(self, tg::Either::Left(directory))
						.await?;
				if let Some(path) = path {
					let edge = directory.get_entry_edge(self, path).await?;
					let edge: tg::graph::Edge<tg::Object> = match edge {
						tg::graph::Edge::Reference(reference) => {
							if reference.kind != tg::artifact::Kind::File {
								return Err(tg::error!("expected a file"));
							}
							tg::graph::Edge::Reference(reference)
						},
						tg::graph::Edge::Object(artifact) => {
							let file = artifact
								.try_unwrap_file()
								.ok()
								.ok_or_else(|| tg::error!("expected a file"))?;
							tg::graph::Edge::Object(file.into())
						},
					};
					let path = dependency
						.0
						.path()
						.map_or_else(|| path.into(), |p| p.join(path));
					let options = tg::referent::Options {
						artifact: dependency.0.artifact().cloned(),
						id: dependency.0.id().cloned(),
						name: dependency.0.name().map(ToOwned::to_owned),
						path: Some(path),
						tag: dependency.0.tag().cloned(),
					};
					tg::Referent {
						item: edge,
						options,
					}
				} else if import.kind.is_none() {
					let item = dependency
						.0
						.item
						.ok_or_else(|| tg::error!("expected a resolved item"))?;
					tg::Referent {
						item,
						options: dependency.0.options,
					}
				} else {
					return Err(tg::error!("expected a root module"));
				}
			},
			(
				None
				| Some(
					tg::module::Kind::Js
					| tg::module::Kind::Ts
					| tg::module::Kind::Dts
					| tg::module::Kind::File,
				),
				tg::Object::File(_),
			)
			| (Some(tg::module::Kind::Object), _)
			| (Some(tg::module::Kind::Blob), tg::Object::Blob(_))
			| (Some(tg::module::Kind::Directory), tg::Object::Directory(_))
			| (Some(tg::module::Kind::Symlink), tg::Object::Symlink(_))
			| (Some(tg::module::Kind::Graph), tg::Object::Graph(_))
			| (Some(tg::module::Kind::Command), tg::Object::Command(_))
			| (Some(tg::module::Kind::Error), tg::Object::Error(_))
			| (
				Some(tg::module::Kind::Artifact),
				tg::Object::Directory(_) | tg::Object::File(_) | tg::Object::Symlink(_),
			) => {
				let item = dependency
					.0
					.item
					.ok_or_else(|| tg::error!("expected a resolved item"))?;
				tg::Referent {
					item,
					options: dependency.0.options,
				}
			},
			(
				None | Some(tg::module::Kind::Js | tg::module::Kind::Ts | tg::module::Kind::Dts),
				_,
			) => {
				return Err(tg::error!("expected a file"));
			},
			(Some(tg::module::Kind::Artifact), _) => {
				return Err(tg::error!("expected an artifact"));
			},
			(Some(tg::module::Kind::Blob), _) => {
				return Err(tg::error!("expected a blob"));
			},
			(Some(tg::module::Kind::Directory), _) => {
				return Err(tg::error!("expected a directory"));
			},
			(Some(tg::module::Kind::File), _) => {
				return Err(tg::error!("expected a file"));
			},
			(Some(tg::module::Kind::Symlink), _) => {
				return Err(tg::error!("expected a symlink"));
			},
			(Some(tg::module::Kind::Graph), _) => {
				return Err(tg::error!("expected a graph"));
			},
			(Some(tg::module::Kind::Command), _) => {
				return Err(tg::error!("expected a command"));
			},
			(Some(tg::module::Kind::Error), _) => {
				return Err(tg::error!("expected an error"));
			},
		};

		let mut referent = referent.map(|edge| tg::module::data::Item::Edge(edge.to_data()));
		referent.inherit(referrer);

		Ok(referent)
	}

	async fn resolve_module_with_path_referrer(
		&self,
		referrer: &tg::Referent<&Path>,
		import: &tg::module::Import,
	) -> tg::Result<tg::Referent<tg::module::data::Item>> {
		let path = import.reference.options().local.as_ref().or(import
			.reference
			.item()
			.try_unwrap_path_ref()
			.ok());
		if let Some(path) = path {
			let path =
				tangram_util::fs::canonicalize_parent(&referrer.item.parent().unwrap().join(path))
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			let metadata = tokio::fs::symlink_metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
			if metadata.is_dir()
				&& matches!(
					import.kind,
					None | Some(tg::module::Kind::Js | tg::module::Kind::Ts)
				) && let Some(root_module_name) =
				tg::package::try_get_root_module_file_name(self, tg::Either::Right(&path)).await?
			{
				let path = path.join(root_module_name);
				let item = tg::module::data::Item::Path(path);
				let referent = tg::Referent::with_item(item);
				return Ok(referent);
			}
			let item = tg::module::data::Item::Path(path);
			let referent = tg::Referent::with_item(item);
			Ok(referent)
		} else {
			// Perform a checkin to ensure the watch is available.
			let arg = tg::checkin::Arg {
				options: tg::checkin::Options {
					unsolved_dependencies: true,
					watch: true,
					..Default::default()
				},
				path: referrer.item().to_path_buf(),
				updates: Vec::new(),
			};
			let context = Context::default();
			let stream = self.checkin_with_context(&context, arg).await?;
			let stream = pin!(stream);
			stream.try_last().await?;

			// Get the watch and retrieve the edge from the graph.
			let entry = self
				.watches
				.iter()
				.find(|entry| referrer.item().starts_with(entry.key()))
				.ok_or_else(|| tg::error!("failed to find a watch for the path"))?;
			let graph = entry.value().get().graph;
			let index = graph
				.paths
				.get(referrer.item())
				.ok_or_else(|| tg::error!("failed to find a node for the path"))?;
			let node = graph.nodes.get(index).unwrap();
			let edge = node.edge.as_ref().unwrap().clone();
			drop(entry);

			// Resolve.
			let referrer = referrer.clone().map(|_| &edge);
			let referent = self
				.resolve_module_with_edge_referrer(&referrer, import)
				.await?;

			Ok(referent)
		}
	}

	pub(crate) async fn handle_resolve_module_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		let output = self.resolve_module_with_context(context, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
