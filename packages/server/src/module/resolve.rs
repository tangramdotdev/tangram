use {crate::Server, std::path::Path, tangram_client as tg, tangram_either::Either};

impl Server {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::module::Data,
		import: &tg::module::Import,
	) -> tg::Result<tg::module::Data> {
		let kind = import.kind;

		// Get the referent.
		let referent = match referrer.referent.item() {
			tg::module::data::Item::Path(path) => {
				let referrer = referrer.referent.clone().map(|_| path.as_ref());
				self.resolve_module_with_path_referrer(&referrer, import)
					.await?
			},
			tg::module::data::Item::Object(object) => {
				let referrer = referrer.referent.clone().map(|_| object);
				self.resolve_module_with_object_referrer(&referrer, import)
					.await?
			},
		};

		// If the kind is not known, then try to infer it from the path extension.
		let kind = if let Some(kind) = kind {
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

				tg::module::data::Item::Object(object) => match &object {
					tg::object::Id::Blob(_) => tg::module::Kind::Blob,
					tg::object::Id::Directory(_) => tg::module::Kind::Directory,
					tg::object::Id::File(_) => tg::module::Kind::File,
					tg::object::Id::Symlink(_) => tg::module::Kind::Symlink,
					tg::object::Id::Graph(_) => tg::module::Kind::Graph,
					tg::object::Id::Command(_) => tg::module::Kind::Command,
				},
			}
		};

		// Create the module.
		let module = tg::module::Data { kind, referent };

		Ok(module)
	}

	async fn resolve_module_with_path_referrer(
		&self,
		referrer: &tg::Referent<&Path>,
		import: &tg::module::Import,
	) -> tg::Result<tg::Referent<tg::module::data::Item>> {
		// Resolve path dependencies.
		if let Some(path) = import.reference.options().local.as_ref().or(import
			.reference
			.item()
			.try_unwrap_path_ref()
			.ok())
		{
			let path =
				crate::util::fs::canonicalize_parent(&referrer.item.parent().unwrap().join(path))
					.await
					.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
			let metadata = tokio::fs::metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
			if metadata.is_dir()
				&& matches!(
					import.kind,
					None | Some(tg::module::Kind::Js | tg::module::Kind::Ts)
				) {
				if let Some(root_module_name) =
					tg::package::try_get_root_module_file_name(self, Either::Right(&path)).await?
				{
					let path = path.join(root_module_name);
					let item = tg::module::data::Item::Path(path);
					let referent = tg::Referent::with_item(item);
					return Ok(referent);
				}
			}
			let item = tg::module::data::Item::Path(path);
			let referent = tg::Referent::with_item(item);
			return Ok(referent);
		}

		Err(tg::error!("cannot resolve non-path dependency from path"))
	}

	async fn resolve_module_with_object_referrer(
		&self,
		referrer: &tg::Referent<&tg::object::Id>,
		import: &tg::module::Import,
	) -> Result<tg::Referent<tg::module::data::Item>, tg::Error> {
		let referrer_object = tg::Object::with_id(referrer.item.clone());
		let file =
			referrer_object.clone().try_unwrap_file().ok().ok_or_else(
				|| tg::error!(%referrer = referrer.item, "the referrer must be a file"),
			)?;
		let referent = file.get_dependency(self, &import.reference).await?;
		let referent = self
			.try_resolve_module_with_kind(import.kind, referent)
			.await?;
		let mut referent = referent.map(|item| tg::module::data::Item::Object(item.id()));
		referent.inherit(referrer);
		Ok(referent)
	}

	async fn try_resolve_module_with_kind(
		&self,
		kind: Option<tg::module::Kind>,
		referent: tg::Referent<tg::Object>,
	) -> tg::Result<tg::Referent<tg::Object>> {
		match (kind, referent.item()) {
			(
				None | Some(tg::module::Kind::Js | tg::module::Kind::Ts),
				tg::Object::Directory(directory),
			) => {
				let path =
					tg::package::try_get_root_module_file_name(self, Either::Left(referent.item()))
						.await?;
				let (item, path) = if let Some(path) = path {
					let file = directory
						.get(self, path)
						.await?
						.try_unwrap_file_ref()
						.map_err(|_| tg::error!("expected a file"))?
						.clone()
						.into();
					(file, path)
				} else if kind.is_none() {
					return Ok(referent);
				} else {
					return Err(tg::error!("expected a root module"));
				};
				let path = referent
					.path()
					.map_or_else(|| path.into(), |p| p.join(path));
				let options = tg::referent::Options {
					id: referent.id().cloned(),
					path: Some(path),
					tag: referent.tag().cloned(),
					name: referent.name().map(ToOwned::to_owned),
					process: referent.process().cloned(),
				};
				let referent = tg::Referent { item, options };
				Ok(referent)
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
			| (
				Some(tg::module::Kind::Artifact),
				tg::Object::Directory(_) | tg::Object::File(_) | tg::Object::Symlink(_),
			) => Ok(referent),
			(
				None | Some(tg::module::Kind::Js | tg::module::Kind::Ts | tg::module::Kind::Dts),
				_,
			) => Err(tg::error!("expected a file")),
			(Some(tg::module::Kind::Artifact), _) => Err(tg::error!("expected an artifact")),
			(Some(tg::module::Kind::Blob), _) => Err(tg::error!("expected a blob")),
			(Some(tg::module::Kind::Directory), _) => Err(tg::error!("expected a directory")),
			(Some(tg::module::Kind::File), _) => Err(tg::error!("expected a file")),
			(Some(tg::module::Kind::Symlink), _) => Err(tg::error!("expected a symlink")),
			(Some(tg::module::Kind::Graph), _) => Err(tg::error!("expected a graph")),
			(Some(tg::module::Kind::Command), _) => Err(tg::error!("expected a command")),
		}
	}
}
