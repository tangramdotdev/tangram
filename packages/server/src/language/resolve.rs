use std::path::PathBuf;

use super::Server;
use tangram_client as tg;

impl Server {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::Module,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		match referrer {
			// .d.ts file imports are never resolved, ignore.
			tg::Module::Dts(_) => Err(tg::error!("unimplemented")),

			// Artifacts are forbidden from importing.
			tg::Module::Artifact(_)
			| tg::Module::File(_)
			| tg::Module::Directory(_)
			| tg::Module::Symlink(_)
			| tg::Module::Js(tg::module::Js::File(_))
			| tg::Module::Ts(tg::module::Js::File(_)) => {
				Err(tg::error!("cannot import from within an artifact"))
			},

			// Resolve package artifact imports.
			tg::Module::Js(tg::module::Js::PackageArtifact(referrer))
			| tg::Module::Ts(tg::module::Js::PackageArtifact(referrer)) => {
				self.resolve_package_artifact(referrer, import).await
			},

			// Resolve package path imports.
			tg::Module::Js(tg::module::Js::PackagePath(referrer))
			| tg::Module::Ts(tg::module::Js::PackagePath(referrer)) => {
				self.resolve_package_path(referrer, import).await
			},
		}
	}

	async fn resolve_package_artifact(
		&self,
		referrer: &tg::module::PackageArtifact,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		let package = tg::Directory::with_id(
			referrer
				.artifact
				.clone()
				.try_into()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?,
		);
		let lock = tg::Lock::with_id(referrer.lock.clone());

		// Get the target of the import.
		match &import.specifier {
			tg::import::Specifier::Path(path) => {
				// Find the module within this package.
				let path = referrer
					.path
					.clone()
					.parent()
					.join(path.clone())
					.normalize();
				self.get_module_within_package(package, lock, &path, import.r#type)
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
				self.get_module_for_package(package, lock, import.r#type)
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
				// Resolve this module within the same package path.
				let path = referrer
					.path
					.clone()
					.parent()
					.join(path.clone())
					.normalize();
				self.get_module_within_path(&referrer.package_path, &path, import.r#type)
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
				self.get_module_for_package(package, lock, import.r#type)
					.await
			},
		}
	}

	#[allow(clippy::ptr_arg)]
	async fn get_module_within_path(
		&self,
		package_path: &PathBuf,
		module_path: &tg::Path,
		r#type: Option<tg::import::Type>,
	) -> tg::Result<tg::Module> {
		let module_absolute_path = package_path.clone().join(module_path).try_into()?;
		let metadata = tokio::fs::symlink_metadata(&module_absolute_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get module metadata"))?;
		let target = match r#type {
			None | Some(tg::import::Type::Artifact) => {
				tg::Module::Artifact(tg::module::Artifact::Path(module_absolute_path))
			},
			Some(tg::import::Type::Directory) if metadata.file_type().is_dir() => {
				tg::Module::Directory(tg::module::Directory::Path(module_absolute_path))
			},
			Some(tg::import::Type::Directory) => {
				return Err(tg::error!("expected a directory"));
			},
			Some(tg::import::Type::File) if metadata.file_type().is_file() => {
				tg::Module::File(tg::module::File::Path(module_absolute_path))
			},
			Some(tg::import::Type::File) => {
				return Err(tg::error!("expected a file"));
			},
			Some(tg::import::Type::Symlink) if metadata.file_type().is_symlink() => {
				tg::Module::Symlink(tg::module::Symlink::Path(module_absolute_path))
			},
			Some(tg::import::Type::Symlink) => {
				return Err(tg::error!("expected a symlink"));
			},
			Some(tg::import::Type::Js) => {
				let package_path = tg::module::PackagePath {
					package_path: package_path.clone(),
					path: module_path.clone(),
				};
				tg::Module::Js(tg::module::Js::PackagePath(package_path))
			},
			Some(tg::import::Type::Ts) => {
				let package_path = tg::module::PackagePath {
					package_path: package_path.clone(),
					path: module_path.clone(),
				};
				tg::Module::Ts(tg::module::Js::PackagePath(package_path))
			},
			_ => return Err(tg::error!("invalid type for import within a package path")),
		};
		Ok(target)
	}

	async fn get_module_for_package(
		&self,
		package: tg::Artifact,
		lock: tg::Lock,
		r#type: Option<tg::import::Type>,
	) -> tg::Result<tg::Module> {
		let package_id = package.id(&self.server, None).await?;
		let target = match r#type {
			Some(tg::import::Type::Artifact) => {
				tg::Module::Artifact(tg::module::Artifact::Id(package_id))
			},
			Some(tg::import::Type::Directory) => {
				let artifact = package_id
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				tg::Module::Directory(tg::module::Directory::Id(artifact))
			},
			Some(tg::import::Type::File) => {
				let artifact = package_id
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				tg::Module::File(tg::module::File::Id(artifact))
			},
			Some(tg::import::Type::Symlink) => {
				let artifact = package_id
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a symlink"))?;
				tg::Module::Symlink(tg::module::Symlink::Id(artifact))
			},
			Some(tg::import::Type::Js) => tg::Module::Js(tg::module::Js::PackageArtifact(
				self.create_package_artifact(package, lock).await?,
			)),
			Some(tg::import::Type::Ts) => tg::Module::Ts(tg::module::Js::PackageArtifact(
				self.create_package_artifact(package, lock).await?,
			)),
			None => tg::Module::from_package(&self.server, &package, &lock).await?,
			_ => return Err(tg::error!("invalid type for import from a package")),
		};
		Ok(target)
	}

	async fn get_module_within_package(
		&self,
		package: tg::Directory,
		lock: tg::Lock,
		module_path: &tg::Path,
		r#type: Option<tg::import::Type>,
	) -> tg::Result<tg::Module> {
		let artifact = package
			.get(&self.server, module_path)
			.await
			.map_err(|source| tg::error!(!source, %path = module_path, %package, "failed to find the artifact within the package"))?
			.id(&self.server, None)
			.await?;
		let target = match r#type {
			None | Some(tg::import::Type::Artifact) => {
				tg::Module::Artifact(tg::module::Artifact::Id(artifact))
			},
			Some(tg::import::Type::Directory) => tg::Module::Directory(tg::module::Directory::Id(
				artifact
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?,
			)),
			Some(tg::import::Type::File) => tg::Module::File(tg::module::File::Id(
				artifact
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?,
			)),
			Some(tg::import::Type::Symlink) => tg::Module::Symlink(tg::module::Symlink::Id(
				artifact
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?,
			)),
			Some(tg::import::Type::Js) => {
				let package_artifact = tg::module::PackageArtifact {
					artifact: package.id(&self.server, None).await?.into(),
					lock: lock.id(&self.server, None).await?,
					path: module_path.clone(),
				};
				tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
			},
			Some(tg::import::Type::Ts) => {
				let package_artifact = tg::module::PackageArtifact {
					artifact: package.id(&self.server, None).await?.into(),
					lock: lock.id(&self.server, None).await?,
					path: module_path.clone(),
				};
				tg::Module::Js(tg::module::Js::PackageArtifact(package_artifact))
			},
			_ => return Err(tg::error!("invalid type for import within a package")),
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
		Ok(tg::module::PackageArtifact {
			artifact,
			lock,
			path,
		})
	}
}
