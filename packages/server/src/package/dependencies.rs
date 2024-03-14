use crate::Server;
use std::collections::{BTreeSet, HashSet, VecDeque};
use tangram_client as tg;
use tangram_error::{error, Result};

impl Server {
	pub(super) async fn get_package_dependencies(
		&self,
		package: &tg::Directory,
	) -> Result<Vec<tg::Dependency>> {
		// Create the dependencies set.
		let mut dependencies: BTreeSet<tg::Dependency> = BTreeSet::default();

		// Get the root module path.
		let root_module_path = tg::package::get_root_module_path(self, package).await?;

		// Create a queue of module paths to visit and a visited set.
		let mut queue: VecDeque<tg::Path> = VecDeque::from(vec![root_module_path]);
		let mut visited: HashSet<tg::Path, fnv::FnvBuildHasher> = HashSet::default();

		// Visit each module.
		while let Some(module_path) = queue.pop_front() {
			// Get the file.
			let file = package
				.get(self, &module_path.clone())
				.await?
				.try_unwrap_file()
				.ok()
				.ok_or_else(|| error!("expected the module to be a file"))?;
			let text: String = file.text(self).await?;

			// Analyze the module.
			let analysis = crate::language::Server::analyze_module(text)
				.map_err(|error| error!(source = error, "failed to analyze the module"))?;

			// Recurse into the dependencies.
			for import in &analysis.imports {
				if let tg::Import::Dependency(dependency) = import {
					let mut dependency = dependency.clone();

					// Normalize the path dependency to be relative to the root.
					dependency.path = dependency
						.path
						.take()
						.map(|path| module_path.clone().parent().join(path).normalize());

					dependencies.insert(dependency.clone());
				}
			}

			// Add the module path to the visited set.
			visited.insert(module_path.clone());

			// Add the unvisited module imports to the queue.
			for import in &analysis.imports {
				if let tg::Import::Module(import) = import {
					let imported_module_path = module_path
						.clone()
						.parent()
						.join(import.clone())
						.normalize();
					if !visited.contains(&imported_module_path) {
						queue.push_back(imported_module_path);
					}
				}
			}
		}

		Ok(dependencies.into_iter().collect())
	}
}
