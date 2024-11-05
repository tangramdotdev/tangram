use super::Compiler;
use std::path::Path;
use tangram_client as tg;
use tangram_either::Either;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::module::Data,
		import: &tg::Import,
	) -> tg::Result<tg::module::Data> {
		let kind = import.kind;

		// Get the referent.
		let referent = match referrer {
			// Handle a path referrer.
			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(package),
						subpath,
						..
					},
				..
			} => {
				self.resolve_import_with_path_referrer(package, subpath.as_deref(), import)
					.await?
			},

			// Handle an object referrer.
			tg::module::Data {
				referent:
					tg::Referent {
						item: tg::module::data::Item::Object(object),
						subpath,
						..
					},
				..
			} => {
				let object = if let Some(subpath) = subpath {
					let tg::object::Id::Directory(directory) = object else {
						return Err(tg::error!("object with subpath must be a directory"));
					};
					let directory = tg::Directory::with_id(directory.clone());
					let artifact = directory
						.get(&self.server, subpath)
						.await
						.map_err(|source| tg::error!(!source, "failed to get directory entry"))?;
					artifact.id(&self.server).await?.clone().into()
				} else {
					object.clone()
				};
				let file = object
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("the referrer must be a file"))?;
				let file = tg::File::with_id(file.clone());
				let referent = file.get_dependency(&self.server, &import.reference).await?;
				let object = referent.item.id(&self.server).await?.clone();
				let item = tg::module::data::Item::Object(object);
				let subpath = referent.subpath;
				let tag = referent.tag;
				tg::Referent { item, subpath, tag }
			},
		};

		// If the kind is not known and the referent is a directory with a root module, then use its kind.
		let kind =
			if kind.is_some() {
				kind
			} else {
				match &referent.item {
					tg::module::data::Item::Path(path) => {
						let path = if let Some(subpath) = &referent.subpath {
							path.join(subpath)
						} else {
							path.clone()
						};
						let metadata = tokio::fs::metadata(&path)
							.await
							.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
						if metadata.is_dir() {
							let package = Either::Right(path.as_ref());
							if let Some(name) =
								tg::package::try_get_root_module_file_name(&self.server, package)
									.await?
							{
								let name = Path::new(name);
								let extension = name.extension();
								if extension.is_some_and(|extension| extension == "js") {
									Some(tg::module::Kind::Js)
								} else if extension.is_some_and(|extension| extension == "ts") {
									Some(tg::module::Kind::Ts)
								} else {
									None
								}
							} else {
								None
							}
						} else {
							None
						}
					},

					tg::module::data::Item::Object(object) => {
						let object =
							if let Some(subpath) = &referent.subpath {
								let object = tg::Object::with_id(object.clone());
								let directory = object
									.try_unwrap_directory_ref()
									.ok()
									.ok_or_else(|| tg::error!("expected a directory"))?;
								let artifact = directory.get(&self.server, subpath).await.map_err(
									|source| tg::error!(!source, "failed to get directory entry"),
								)?;
								let artifact = artifact.id(&self.server).await?.clone();
								artifact.into()
							} else {
								object.clone()
							};
						if object.try_unwrap_directory_ref().is_ok() {
							let object = tg::Object::with_id(object);
							let package = Either::Left(&object);
							if let Some(name) =
								tg::package::try_get_root_module_file_name(&self.server, package)
									.await?
							{
								let name = Path::new(name);
								let extension = name.extension();
								if extension.is_some_and(|extension| extension == "js") {
									Some(tg::module::Kind::Js)
								} else if extension.is_some_and(|extension| extension == "ts") {
									Some(tg::module::Kind::Ts)
								} else {
									None
								}
							} else {
								None
							}
						} else {
							None
						}
					},
				}
			};

		// If the kind is not known, then try to infer it from the path extension.
		let kind = if let Some(kind) = kind {
			Some(kind)
		} else if let Some(subpath) = &referent.subpath {
			let extension = subpath.extension();
			if extension.is_some_and(|extension| extension == "js") {
				Some(tg::module::Kind::Js)
			} else if extension.is_some_and(|extension| extension == "ts") {
				Some(tg::module::Kind::Ts)
			} else {
				None
			}
		} else if let tg::module::data::Item::Path(path) = &referent.item {
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

		// Finally, if the kind is not known, then infer it from the object's kind.
		let kind =
			if let Some(kind) = kind {
				kind
			} else {
				match &referent.item {
					tg::module::data::Item::Path(path) => {
						let path = if let Some(subpath) = &referent.subpath {
							path.join(subpath)
						} else {
							path.clone()
						};
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

					tg::module::data::Item::Object(object) => {
						let object =
							if let Some(subpath) = &referent.subpath {
								let object = tg::Object::with_id(object.clone());
								let directory = object
									.try_unwrap_directory_ref()
									.ok()
									.ok_or_else(|| tg::error!("expected a directory"))?;
								let artifact = directory.get(&self.server, subpath).await.map_err(
									|source| tg::error!(!source, "failed to get directory entry"),
								)?;
								let artifact = artifact.id(&self.server).await?.clone();
								artifact.into()
							} else {
								object.clone()
							};
						match &object {
							tg::object::Id::Leaf(_) => tg::module::Kind::Leaf,
							tg::object::Id::Branch(_) => tg::module::Kind::Branch,
							tg::object::Id::Directory(_) => tg::module::Kind::Directory,
							tg::object::Id::File(_) => tg::module::Kind::File,
							tg::object::Id::Symlink(_) => tg::module::Kind::Symlink,
							tg::object::Id::Graph(_) => tg::module::Kind::Graph,
							tg::object::Id::Target(_) => tg::module::Kind::Target,
						}
					},
				}
			};

		// Create the module.
		let module = tg::module::Data { kind, referent };

		Ok(module)
	}

	// Given a package at a path and module at some subpath within it, resolve an import relative to the module.
	async fn resolve_import_with_path_referrer(
		&self,
		package: &Path,
		subpath: Option<&Path>,
		import: &tg::Import,
	) -> tg::Result<tg::Referent<tg::module::data::Item>> {
		// Get the referrer within some lockfile.
		let subpath = subpath.unwrap_or("".as_ref());
		let module_path = package.join(subpath);

		// Get the lockfile and its path.
		let (lockfile_path, lockfile) = 'a: {
			// Search the ancestors for a lockfile, if it exists.
			for ancestor in module_path.ancestors().skip(1) {
				// Check if the lockfile exists.
				let lockfile_path = ancestor.join(tg::package::LOCKFILE_FILE_NAME);
				let exists = tokio::fs::try_exists(&lockfile_path).await.map_err(
					|source| tg::error!(!source, %package = ancestor.display(), "failed to check if lockfile exists"),
				)?;
				if !exists {
					continue;
				}

				// Parse the lockfile.
				let contents = tokio::fs::read_to_string(&lockfile_path).await.map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to read lockfile"),
				)?;
				let lockfile = serde_json::from_str::<tg::Lockfile>(&contents).map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to deserialize lockfile"),
				)?;
				break 'a (lockfile_path, lockfile);
			}

			// Error if no lockfile is found.
			return Err(tg::error!(%module = module_path.display(), "failed to find lockfile"));
		};

		// Find the referrer in the lockfile.
		let module_index = self
			.server
			.find_node_index_in_lockfile(&module_path, &lockfile_path, &lockfile)
			.await?;

		// The module within the lockfile must be a file for it to have imports.
		let tg::lockfile::Node::File { dependencies, .. } = &lockfile.nodes[module_index] else {
			return Err(
				tg::error!(%lockfile = lockfile_path.display(), %node = module_index, "expected a file node"),
			);
		};

		// Get the import path.
		let import_path = import
			.reference
			.item()
			.try_unwrap_path_ref()
			.ok()
			.or_else(|| import.reference.options()?.path.as_ref());

		match dependencies.get(&import.reference) {
			// If this points to another node in the lockfile, find it within the lockfile.
			Some(tg::Referent {
				item: Either::Left(index),
				subpath,
				tag,
			}) => {
				let package_path = self
					.server
					.find_path_in_lockfile(*index, &lockfile_path, &lockfile)
					.await?;
				Ok(tg::Referent {
					item: tg::module::data::Item::Path(package_path),
					subpath: subpath.clone(),
					tag: tag.clone(),
				})
			},

			// Resolve objects normally.
			Some(tg::Referent {
				item: Either::Right(object),
				subpath,
				tag,
			}) => Ok(tg::Referent {
				item: tg::module::data::Item::Object(object.clone()),
				subpath: subpath.clone(),
				tag: tag.clone(),
			}),

			None => {
				// If this is a path dependency but not in the dependencies table for the node, then it is an artifact dependency.
				if let Some(import_path) = import_path {
					let item = module_path.join(import_path);
					return Ok(tg::Referent {
						item: tg::module::data::Item::Path(item),
						subpath: import
							.reference
							.options()
							.and_then(|options| options.subpath.clone()),
						tag: None,
					});
				};

				// Otherwise we cannot resolve this reference.
				Err(
					tg::error!(%module = module_path.display(), %import = import.reference, "failed to resolve import"),
				)
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{compiler::Compiler, Config, Server};
	use futures::FutureExt;
	use std::{
		future::Future,
		panic::AssertUnwindSafe,
		path::{Path, PathBuf},
	};
	use tangram_client as tg;
	use tangram_temp::{self as temp, directory, Temp};

	#[tokio::test]
	async fn path_dependency() -> tg::Result<()> {
		test_module(
			directory!({
				"tangram.ts": r#"import * as foo from "./foo.tg.ts""#,
				"foo.tg.ts": "",
			}),
			tg::module::Kind::Ts,
			"tangram.ts",
			tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap(),
			|_, module| async move {
				assert_eq!(module.kind, tg::module::Kind::Ts);
				let tg::module::data::Item::Object(_object) = &module.referent.item else {
					return Err(tg::error!("expected a path item"));
				};
				let Some(subpath) = &module.referent.subpath else {
					return Err(tg::error!("expected a subpath"));
				};
				let expected: &Path = "./foo.tg.ts".as_ref();
				assert_eq!(subpath, expected);
				Ok::<_, tg::Error>(())
			},
		)
		.await
	}

	#[tokio::test]
	async fn lsp_path_dependency() -> tg::Result<()> {
		test_lsp_module(
			directory!({
				"tangram.ts": r#"import * as foo from "./foo.tg.ts""#,
				"foo.tg.ts": "",
			}),
			tg::module::Kind::Ts,
			"tangram.ts",
			tg::Import::with_specifier_and_attributes("./foo.tg.ts", None).unwrap(),
			|_, module| async move {
				assert_eq!(module.kind, tg::module::Kind::Ts);
				let tg::module::data::Item::Path(_path) = &module.referent.item else {
					return Err(tg::error!("expected a path item"));
				};
				let Some(subpath) = &module.referent.subpath else {
					return Err(tg::error!("expected a subpath"));
				};
				let expected: &Path = "./foo.tg.ts".as_ref();
				assert_eq!(subpath, expected);
				Ok::<_, tg::Error>(())
			},
		)
		.await
	}

	async fn test_module<F, Fut>(
		artifact: temp::Artifact,
		kind: tg::module::Kind,
		subpath: &str,
		import: tg::Import,
		assertions: F,
	) -> tg::Result<()>
	where
		F: FnOnce(Server, tg::module::Data) -> Fut,
		Fut: Future<Output = tg::Result<()>>,
	{
		let temp = Temp::new();
		let options = Config::with_path(temp.path().to_owned());
		let server = Server::start(options).await?;
		let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
		let result = AssertUnwindSafe(async {
			let directory = Temp::new();
			artifact.to_path(directory.as_ref()).await.map_err(
				|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
			)?;

			let artifact = tg::Artifact::check_in(
				&server,
				tg::artifact::checkin::Arg {
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					path: directory.path().to_owned(),
				},
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the artifact"))?;

			let referrer = tg::module::Data {
				kind,
				referent: tg::Referent {
					item: tg::module::data::Item::Object(artifact.id(&server).await?.into()),
					subpath: Some(PathBuf::from(subpath)),
					tag: None,
				},
			};

			std::mem::forget(directory);
			let module = compiler.resolve_module(&referrer, &import).await?;
			(assertions)(server.clone(), module).await?;

			Ok::<_, tg::Error>(())
		})
		.catch_unwind()
		.await;
		server.stop();
		server.wait().await;
		result.unwrap()
	}
	async fn test_lsp_module<F, Fut>(
		artifact: temp::Artifact,
		kind: tg::module::Kind,
		subpath: &str,
		import: tg::Import,
		assertions: F,
	) -> tg::Result<()>
	where
		F: FnOnce(Server, tg::module::Data) -> Fut,
		Fut: Future<Output = tg::Result<()>>,
	{
		let temp = Temp::new();
		let options = Config::with_path(temp.path().to_owned());
		let server = Server::start(options).await?;
		let compiler = Compiler::new(&server, tokio::runtime::Handle::current());
		let result = AssertUnwindSafe(async {
			let directory = Temp::new();
			artifact.to_path(directory.as_ref()).await.map_err(
				|source| tg::error!(!source, %path = directory.path().display(), "failed to write the artifact"),
			)?;

			tg::Artifact::check_in(
				&server,
				tg::artifact::checkin::Arg {
					destructive: false,
					deterministic: false,
					ignore: true,
					locked: false,
					path: directory.path().to_owned(),
				},
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the artifact"))?;

			let referrer = tg::module::Data {
				kind,
				referent: tg::Referent {
					item: tg::module::data::Item::Path(directory.path().to_owned()),
					subpath: Some(PathBuf::from(subpath)),
					tag: None,
				},
			};

			let module = compiler.resolve_module(&referrer, &import).await?;
			(assertions)(server.clone(), module).await?;

			Ok::<_, tg::Error>(())
		})
		.catch_unwind()
		.await;
		server.stop();
		server.wait().await;
		result.unwrap().inspect_err(|error| {
			let trace = error.trace(&server.config.advanced.error_trace_options);
			eprintln!("{trace}");
		})
	}
}
