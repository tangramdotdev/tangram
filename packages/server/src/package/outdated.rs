use crate::Server;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_http::{
	outgoing::{ResponseBuilderExt as _, ResponseExt as _},
	Incoming, Outgoing,
};

impl Server {
	pub async fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<tg::package::outdated::Output> {
		let (package, lock) = tg::package::get_with_lock(self, dependency)
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
		package: tg::Directory,
		lock: tg::Lock,
		visited: &mut BTreeMap<tg::lock::Id, tg::package::outdated::Output>,
	) -> tg::Result<tg::package::outdated::Output> {
		let id = lock.id(self, None).await?;
		if let Some(existing) = visited.get(&id) {
			return Ok(existing.clone());
		}

		// Get the relevent versions of this package.
		let (compatible_versions, all_versions) = if dependency.name.is_some() {
			// Get the current, compatible, and latest versions.
			let compatible_versions = self.try_get_package_versions(dependency).await.map_err(
				|source| tg::error!(!source, %dependency, "failed to get compatible package versions"),
			)?;
			let all_versions = self
				.try_get_package_versions(&tg::Dependency::with_name(
					dependency.name.clone().unwrap(),
				))
				.await
				.map_err(
					|source| tg::error!(!source, %dependency, "failed to get package versions"),
				)?;
			(compatible_versions, all_versions)
		} else {
			(None, None)
		};

		let metadata = tg::package::get_metadata(self, &package).await.ok();
		let current = metadata
			.as_ref()
			.and_then(|metadata| metadata.version.clone());
		let compatible = compatible_versions.and_then(|mut versions| versions.pop());
		let latest = all_versions.and_then(|mut versions| versions.pop());
		let yanked = self.get_package_yanked(&package).await?;
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
				(Some(package), _) => package,
				(None, Some(path)) => package
					.get(self, path)
					.await
					.map_err(
						|source| tg::error!(!source, %path, "could not resolve path dependency"),
					)?
					.try_unwrap_directory()
					.map_err(|source| tg::error!(!source, "expected a directory"))?,
				(None, None) => return Err(tg::error!("invalid lock")),
			};
			let outdated =
				Box::pin(self.get_outdated_inner(&dependency, package, lock, visited)).await?;
			if outdated.info.is_some() || !outdated.dependencies.is_empty() {
				dependencies.insert(dependency.clone(), outdated);
			}
		}
		let outdated = tg::package::outdated::Output { info, dependencies };

		// Mark this package as visited.
		visited.insert(id, outdated.clone());

		Ok(outdated)
	}
}

impl Server {
	pub(crate) async fn handle_outdated_package_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		dependency: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let Ok(dependency) = urlencoding::decode(dependency) else {
			return Ok(http::Response::bad_request());
		};
		let Ok(dependency) = dependency.parse() else {
			return Ok(http::Response::bad_request());
		};

		// Get the outdated dependencies.
		let output = handle.get_package_outdated(&dependency).await?;

		// Create the response.
		let response = http::Response::builder().json(output).unwrap();

		Ok(response)
	}
}
