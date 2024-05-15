use super::Compiler;
use std::path::PathBuf;
use tangram_client as tg;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::Module,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		match referrer {
			tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
			| tg::Module::Ts(tg::module::Js::PackageArtifact(package_artifact)) => {
				self.resolve_package_artifact(package_artifact, import)
					.await
			},

			tg::Module::Js(tg::module::Js::PackagePath(package_path))
			| tg::Module::Ts(tg::module::Js::PackagePath(package_path)) => {
				self.resolve_package_path(package_path, import).await
			},

			tg::Module::Js(tg::module::Js::File(_)) | tg::Module::Ts(tg::module::Js::File(_)) => {
				Err(tg::error!(
					"cannot import from a js or ts module with no package"
				))
			},

			tg::Module::Dts(_) => Err(tg::error!("unimplemented")),

			tg::Module::Artifact(_)
			| tg::Module::File(_)
			| tg::Module::Directory(_)
			| tg::Module::Symlink(_) => Err(tg::error!("cannot import from an artifact")),
		}
	}

	async fn resolve_package_artifact(
		&self,
		referrer: &tg::module::PackageArtifact,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		let id = referrer
			.artifact
			.clone()
			.try_unwrap_directory()
			.ok()
			.ok_or_else(|| tg::error!("expected a directory"))?;
		let package = tg::Directory::with_id(id);
		let lock = tg::Lock::with_id(referrer.lock.clone());
		match &import.specifier {
			tg::import::Specifier::Path(path) => {
				let path = referrer
					.path
					.clone()
					.parent()
					.join(path.clone())
					.normalize();
				self.resolve_package_artifact_with_path(package, lock, &path, import.kind)
					.await
			},

			tg::import::Specifier::Dependency(dependency) => {
				// Make the dependency path relative to the package.
				let mut dependency = dependency.clone();
				if let Some(path) = dependency.path.as_mut() {
					*path = referrer
						.path
						.clone()
						.parent()
						.join(path.clone())
						.normalize();
				}

				// Resolve the dependency using the lock.
				let (package, lock) = lock.get(&self.server, &dependency).await?;
				let package = package.unwrap().into();

				// Create a module for the package.
				self.module_for_package_with_kind(package, lock, import.kind)
					.await
			},
		}
	}

	async fn resolve_package_path(
		&self,
		referrer: &tg::module::PackagePath,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		match &import.specifier {
			tg::import::Specifier::Path(path) => {
				let path = referrer
					.path
					.clone()
					.parent()
					.join(path.clone())
					.normalize();
				self.resolve_package_path_with_path(&referrer.package_path, &path, import.kind)
					.await
			},

			tg::import::Specifier::Dependency(dependency) => {
				// Make the dependency path relative to the package.
				let mut dependency = dependency.clone();
				if let Some(path) = dependency.path.as_mut() {
					*path = referrer
						.path
						.clone()
						.parent()
						.join(path.clone())
						.normalize();
				}

				// Get the package and lock for the dependency.
				let (package, lock) = tg::package::get_with_lock(&self.server, &dependency).await?;

				// Create a module for the package.
				self.module_for_package_with_kind(package, lock, import.kind)
					.await
			},
		}
	}

	#[allow(clippy::ptr_arg)]
	async fn resolve_package_path_with_path(
		&self,
		package_path: &PathBuf,
		module_path: &tg::Path,
		kind: Option<tg::import::Kind>,
	) -> tg::Result<tg::Module> {
		let path = package_path.clone().join(module_path).try_into()?;
		let metadata = tokio::fs::symlink_metadata(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
		let target = match kind {
			None | Some(tg::import::Kind::Artifact) => {
				tg::Module::Artifact(tg::module::Artifact::Path(path))
			},

			Some(tg::import::Kind::Directory) => {
				if !metadata.file_type().is_dir() {
					return Err(tg::error!("expected a directory"));
				}
				tg::Module::Directory(tg::module::Directory::Path(path))
			},

			Some(tg::import::Kind::File) => {
				if !metadata.file_type().is_file() {
					return Err(tg::error!("expected a file"));
				}
				tg::Module::File(tg::module::File::Path(path))
			},

			Some(tg::import::Kind::Symlink) => {
				if !metadata.file_type().is_symlink() {
					return Err(tg::error!("expected a symlink"));
				}
				tg::Module::Symlink(tg::module::Symlink::Path(path))
			},

			Some(tg::import::Kind::Js) => {
				let package_path = tg::module::PackagePath {
					package_path: package_path.clone(),
					path: module_path.clone(),
				};
				tg::Module::Js(tg::module::Js::PackagePath(package_path))
			},

			Some(tg::import::Kind::Ts) => {
				let package_path = tg::module::PackagePath {
					package_path: package_path.clone(),
					path: module_path.clone(),
				};
				tg::Module::Ts(tg::module::Js::PackagePath(package_path))
			},

			_ => return Err(tg::error!("invalid kind for import within a package path")),
		};
		Ok(target)
	}

	async fn module_for_package_with_kind(
		&self,
		package: tg::Artifact,
		lock: tg::Lock,
		kind: Option<tg::import::Kind>,
	) -> tg::Result<tg::Module> {
		let id = package.id(&self.server, None).await?;
		let module = match kind {
			None => tg::Module::with_package_and_lock(&self.server, &package, &lock).await?,

			Some(tg::import::Kind::Js) => tg::Module::Js(tg::module::Js::PackageArtifact(
				self.create_package_artifact(package, lock).await?,
			)),

			Some(tg::import::Kind::Ts) => tg::Module::Ts(tg::module::Js::PackageArtifact(
				self.create_package_artifact(package, lock).await?,
			)),

			Some(tg::import::Kind::Artifact) => tg::Module::Artifact(tg::module::Artifact::Id(id)),

			Some(tg::import::Kind::Directory) => {
				let id = id
					.try_unwrap_directory()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				tg::Module::Directory(tg::module::Directory::Id(id))
			},

			Some(tg::import::Kind::File) => {
				let id = id
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				tg::Module::File(tg::module::File::Id(id))
			},

			Some(tg::import::Kind::Symlink) => {
				let id = id
					.try_unwrap_symlink()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				tg::Module::Symlink(tg::module::Symlink::Id(id))
			},

			_ => {
				return Err(tg::error!("invalid kind for import from a package"));
			},
		};
		Ok(module)
	}

	async fn resolve_package_artifact_with_path(
		&self,
		package: tg::Directory,
		lock: tg::Lock,
		module_path: &tg::Path,
		kind: Option<tg::import::Kind>,
	) -> tg::Result<tg::Module> {
		let artifact = package
			.get(&self.server, module_path)
			.await
			.map_err(|source| tg::error!(!source, %path = module_path, %package, "failed to find the artifact within the package"))?
			.id(&self.server, None)
			.await?;
		let target = match kind {
			Some(tg::import::Kind::Js) => {
				let package_artifact = tg::module::PackageArtifact {
					artifact: package.id(&self.server, None).await?.into(),
					lock: lock.id(&self.server, None).await?,
					path: module_path.clone(),
				};
				tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
			},

			Some(tg::import::Kind::Ts) => {
				let package_artifact = tg::module::PackageArtifact {
					artifact: package.id(&self.server, None).await?.into(),
					lock: lock.id(&self.server, None).await?,
					path: module_path.clone(),
				};
				tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
			},

			None | Some(tg::import::Kind::Artifact) => {
				tg::Module::Artifact(tg::module::Artifact::Id(artifact))
			},

			Some(tg::import::Kind::Directory) => {
				let id = artifact
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				tg::Module::Directory(tg::module::Directory::Id(id))
			},

			Some(tg::import::Kind::File) => {
				let id = artifact
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				tg::Module::File(tg::module::File::Id(id))
			},

			Some(tg::import::Kind::Symlink) => {
				let id = artifact
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				tg::Module::Symlink(tg::module::Symlink::Id(id))
			},

			_ => return Err(tg::error!("invalid kind for import within a package")),
		};
		Ok(target)
	}

	async fn create_package_artifact(
		&self,
		package: tg::Artifact,
		lock: tg::Lock,
	) -> tg::Result<tg::module::PackageArtifact> {
		let artifact = package.id(&self.server, None).await?;
		let path = tg::package::get_root_module_path(&self.server, &package).await?;
		let lock = lock.id(&self.server, None).await?;
		let package_artifact = tg::module::PackageArtifact {
			artifact,
			lock,
			path,
		};
		Ok(package_artifact)
	}
}
