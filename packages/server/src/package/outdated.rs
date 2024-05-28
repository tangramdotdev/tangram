use crate::Server;
use std::collections::BTreeMap;
use tangram_client::{self as tg, Handle as _};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::outdated::Arg,
	) -> tg::Result<tg::package::outdated::Output> {
		let (package, lock) = tg::package::get_with_lock(self, dependency, arg.locked)
			.await
			.map_err(|source| tg::error!(!source, %dependency, "failed to get package and lock"))?;
		let mut visited = BTreeMap::new();
		let outdated = self
			.get_outdated_inner(dependency, package, lock, &mut visited)
			.await?;
		Ok(outdated)
	}

	pub async fn get_outdated_inner(
		&self,
		dependency: &tg::Dependency,
		artifact: tg::Artifact,
		lock: tg::Lock,
		visited: &mut BTreeMap<tg::lock::Id, tg::package::outdated::Output>,
	) -> tg::Result<tg::package::outdated::Output> {
		// Use an existing output if possible.
		let lock_id = lock.id(self, None).await?;
		if let Some(existing) = visited.get(&lock_id) {
			return Ok(existing.clone());
		}

		// Get the relevent versions of this package.
		let (compatible_versions, all_versions) = if dependency.name.is_some() {
			let compatible_versions = self.try_get_package_versions(dependency).await.map_err(
				|source| tg::error!(!source, %dependency, "failed to get compatible package versions"),
			)?;
			let dependency_with_name = tg::Dependency::with_name(dependency.name.clone().unwrap());
			let all_versions = self
				.try_get_package_versions(&dependency_with_name)
				.await
				.map_err(
					|source| tg::error!(!source, %dependency, "failed to get package versions"),
				)?;
			(compatible_versions, all_versions)
		} else {
			(None, None)
		};

		// Create the outdated info.
		let id = artifact.id(self, None).await?;
		let dependency = tg::Dependency::with_artifact(id);
		let arg = tg::package::get::Arg {
			metadata: true,
			yanked: true,
			..Default::default()
		};
		let output = self.get_package(&dependency, arg).await?;
		let current = output
			.metadata
			.as_ref()
			.and_then(|metadata| metadata.version.clone());
		let compatible = compatible_versions.and_then(|mut versions| versions.pop());
		let latest = all_versions.and_then(|mut versions| versions.pop());
		let yanked = output
			.yanked
			.ok_or_else(|| tg::error!("expected yanked to be set"))?;
		let info = (current != latest && latest.is_some() || yanked).then(|| {
			tg::package::outdated::Info {
				current: current.unwrap(),
				compatible: compatible.unwrap(),
				latest: latest.unwrap(),
				yanked,
			}
		});

		// Visit every dependency.
		let mut dependencies = BTreeMap::new();
		for dependency in lock.dependencies(self).await? {
			let (child_package, lock) = lock.get(self, &dependency).await?;
			let package = match (child_package, &dependency.path) {
				(Some(package), _) => package.into(),
				(None, Some(path)) => artifact
					.try_unwrap_directory_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?
					.get(self, path)
					.await
					.map_err(
						|source| tg::error!(!source, %path, "could not resolve path dependency"),
					)?,
				(None, None) => return Err(tg::error!("invalid lock")),
			};
			let outdated =
				Box::pin(self.get_outdated_inner(&dependency, package, lock, visited)).await?;
			if outdated.info.is_some() || !outdated.dependencies.is_empty() {
				dependencies.insert(dependency.clone(), outdated);
			}
		}

		// Create the output.
		let output = tg::package::outdated::Output { info, dependencies };

		// Mark this package as visited.
		visited.insert(lock_id, output.clone());

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_outdated_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		dependency: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let Ok(dependency) = urlencoding::decode(dependency) else {
			return Ok(http::Response::builder().bad_request().empty().unwrap());
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(http::Response::builder().bad_request().empty().unwrap());
		};
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.get_package_outdated(&dependency, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
