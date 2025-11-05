use {
	dashmap::DashMap,
	std::{
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_either::Either,
};

/// Resolve an import from a module.
pub async fn resolve<H>(
	handle: &H,
	referrer: &tg::module::Data,
	import: &tg::module::Import,
	locks: Option<&DashMap<PathBuf, (Arc<tg::graph::Data>, u64)>>,
) -> tg::Result<tg::module::Data>
where
	H: tg::Handle,
{
	let kind = import.kind;

	// Get the referent.
	let referent = match referrer.referent.item() {
		tg::module::data::Item::Path(path) => {
			let referrer = referrer.referent.clone().map(|_| path.as_ref());
			resolve_module_with_path_referrer(handle, &referrer, import, locks).await?
		},
		tg::module::data::Item::Object(object) => {
			let referrer = referrer.referent.clone().map(|_| object);
			resolve_module_with_object_referrer(handle, &referrer, import).await?
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

async fn resolve_module_with_path_referrer<H>(
	handle: &H,
	referrer: &tg::Referent<&Path>,
	import: &tg::module::Import,
	locks: Option<&DashMap<PathBuf, (Arc<tg::graph::Data>, u64)>>,
) -> tg::Result<tg::Referent<tg::module::data::Item>>
where
	H: tg::Handle,
{
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
			tg::package::try_get_root_module_file_name(handle, Either::Right(&path)).await?
		{
			let path = path.join(root_module_name);
			let item = tg::module::data::Item::Path(path);
			let referent = tg::Referent::with_item(item);
			return Ok(referent);
		}
		let item = tg::module::data::Item::Path(path);
		let referent = tg::Referent::with_item(item);
		return Ok(referent);
	}

	// Find the lockfile by searching ancestor directories.
	let referrer_path = referrer.item;
	let mut lockfile_path = None;
	for ancestor in referrer_path.parent().unwrap().ancestors() {
		let candidate = ancestor.join(tg::package::LOCKFILE_FILE_NAME);
		if tokio::fs::try_exists(&candidate).await.unwrap_or(false) {
			lockfile_path = Some(candidate);
			break;
		}
	}
	let lockfile_path = lockfile_path.ok_or_else(|| tg::error!("failed to find a lockfile"))?;
	let package_path = lockfile_path.parent().unwrap();

	// Get the lockfile mtime.
	let metadata = tokio::fs::metadata(&lockfile_path)
		.await
		.map_err(|source| tg::error!(!source, "failed to get the lockfile metadata"))?;
	let mtime = metadata
		.modified()
		.map_err(|source| tg::error!(!source, "failed to get the mtime"))?
		.duration_since(std::time::UNIX_EPOCH)
		.map_err(|source| tg::error!(!source, "failed to compute the duration"))?
		.as_secs();

	// Try to get a fresh entry from the cache.
	let cached_lock = if let Some(locks) = locks {
		locks.get(&lockfile_path).and_then(|entry| {
			let (cached_lock, cached_mtime) = entry.value();
			if *cached_mtime == mtime {
				Some(cached_lock.clone())
			} else {
				None
			}
		})
	} else {
		None
	};

	// If we have a fresh cached value, use it; otherwise read from disk.
	let lock = if let Some(lock) = cached_lock {
		lock
	} else {
		let contents = tokio::fs::read(&lockfile_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the lockfile"))?;
		let lock = tg::graph::Data::deserialize(contents.as_slice())
			.map_err(|source| tg::error!(!source, "failed to deserialize the lockfile"))?;
		let lock = Arc::new(lock);

		// Update the cache if we have one.
		if let Some(locks) = locks {
			locks.insert(lockfile_path.clone(), (lock.clone(), mtime));
		}

		lock
	};

	// Find the node in the lock that corresponds to the referrer's path.
	let mut current_node = 0;
	let relative_path = referrer_path
		.strip_prefix(package_path)
		.map_err(|source| tg::error!(!source, "failed to get the relative path"))?;
	for component in relative_path.components() {
		let name = component
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("invalid path component"))?;
		let node = &lock.nodes[current_node];
		let directory = node
			.try_unwrap_directory_ref()
			.map_err(|_| tg::error!("expected a directory node"))?;
		let edge = directory
			.entries
			.get(name)
			.ok_or_else(|| tg::error!("the path was not found in the lock"))?;
		let reference = edge
			.try_unwrap_reference_ref()
			.map_err(|_| tg::error!("expected a reference"))?;
		current_node = reference.node;
	}

	// Look up the reference in the file node's dependencies.
	let node = &lock.nodes[current_node];
	let file = node
		.try_unwrap_file_ref()
		.map_err(|_| tg::error!("expected a file node"))?;
	let referent = file
		.dependencies
		.get(&import.reference)
		.ok_or_else(|| tg::error!("the dependency was not found in the lock"))?
		.as_ref()
		.ok_or_else(|| tg::error!("the dependency is None"))?;

	// Get the object from the edge.
	let object_id = match referent.item() {
		tg::graph::data::Edge::Reference(_) => {
			return Err(tg::error!(
				"unexpected reference in the lockfile dependency"
			));
		},
		tg::graph::data::Edge::Object(object_id) => object_id.clone(),
	};

	// Create an object referent and resolve to the root module if it's a directory.
	let object = tg::Object::with_id(object_id);
	let object_referent = tg::Referent {
		item: object,
		options: referent.options().clone(),
	};
	let resolved = try_resolve_module_with_kind(handle, import.kind, object_referent).await?;
	let result = resolved.map(|item| tg::module::data::Item::Object(item.id()));

	Ok(result)
}

async fn resolve_module_with_object_referrer<H>(
	handle: &H,
	referrer: &tg::Referent<&tg::object::Id>,
	import: &tg::module::Import,
) -> Result<tg::Referent<tg::module::data::Item>, tg::Error>
where
	H: tg::Handle,
{
	let referrer_object = tg::Object::with_id(referrer.item.clone());
	let file = referrer_object
		.clone()
		.try_unwrap_file()
		.ok()
		.ok_or_else(|| tg::error!(%referrer = referrer.item, "the referrer must be a file"))?;
	let referent = file.get_dependency(handle, &import.reference).await?;
	let referent = try_resolve_module_with_kind(handle, import.kind, referent).await?;
	let mut referent = referent.map(|item| tg::module::data::Item::Object(item.id()));
	referent.inherit(referrer);
	Ok(referent)
}

async fn try_resolve_module_with_kind<H>(
	handle: &H,
	kind: Option<tg::module::Kind>,
	referent: tg::Referent<tg::Object>,
) -> tg::Result<tg::Referent<tg::Object>>
where
	H: tg::Handle,
{
	match (kind, referent.item()) {
		(
			None | Some(tg::module::Kind::Js | tg::module::Kind::Ts),
			tg::Object::Directory(directory),
		) => {
			let path =
				tg::package::try_get_root_module_file_name(handle, Either::Left(directory)).await?;
			let (item, path) = if let Some(path) = path {
				let file = directory
					.get(handle, path)
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
				name: referent.name().map(ToOwned::to_owned),
				path: Some(path),
				tag: referent.tag().cloned(),
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
		(None | Some(tg::module::Kind::Js | tg::module::Kind::Ts | tg::module::Kind::Dts), _) => {
			Err(tg::error!("expected a file"))
		},
		(Some(tg::module::Kind::Artifact), _) => Err(tg::error!("expected an artifact")),
		(Some(tg::module::Kind::Blob), _) => Err(tg::error!("expected a blob")),
		(Some(tg::module::Kind::Directory), _) => Err(tg::error!("expected a directory")),
		(Some(tg::module::Kind::File), _) => Err(tg::error!("expected a file")),
		(Some(tg::module::Kind::Symlink), _) => Err(tg::error!("expected a symlink")),
		(Some(tg::module::Kind::Graph), _) => Err(tg::error!("expected a graph")),
		(Some(tg::module::Kind::Command), _) => Err(tg::error!("expected a command")),
	}
}
