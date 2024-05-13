use crate::Server;
use std::collections::{BTreeSet, HashSet, VecDeque};
use tangram_client as tg;

impl Server {
	pub(super) async fn get_package_dependencies(
		&self,
		package: &tg::Directory,
	) -> tg::Result<Vec<tg::Dependency>> {
		// Create the dependencies set.
		let mut dependencies: BTreeSet<tg::Dependency> = BTreeSet::default();

		// Get the root module path.
		let root_module_path =
			tg::package::get_root_module_path(self, &package.clone().into()).await?;

		// Create a queue of module paths to visit and a visited set.
		let r#type = tg::import::Type::try_from_path(&root_module_path);
		let mut queue = VecDeque::from(vec![(root_module_path, r#type)]);
		let mut visited: HashSet<(tg::Path, Option<tg::import::Type>), fnv::FnvBuildHasher> =
			HashSet::default();

		// Visit each module.
		while let Some((module_path, r#type)) = queue.pop_front() {
			// Get the file.
			let file = package
				.get(self, &module_path.clone())
				.await?
				.try_unwrap_file()
				.ok()
				.ok_or_else(|| tg::error!("expected the module to be a file"))?;
			let text: String = file.text(self).await?;

			// Add the module path to the visited set.
			visited.insert((module_path.clone(), r#type));

			// If this is not a JavaScript or TypeScript module, then continue.
			if !matches!(r#type, Some(tg::import::Type::Js | tg::import::Type::Ts)) {
				continue;
			}

			// Analyze the module.
			let analysis = crate::compiler::Compiler::analyze_module(text)
				.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

			// Recurse into the dependencies.
			let iter = analysis.imports.iter().filter_map(|import| {
				if let tg::import::Specifier::Dependency(dependency) = &import.specifier {
					Some(dependency.clone())
				} else {
					None
				}
			});
			for mut dependency in iter {
				// Normalize the path dependency to be relative to the root.
				dependency.path = dependency
					.path
					.take()
					.map(|path| module_path.clone().parent().join(path).normalize());

				dependencies.insert(dependency.clone());
			}

			// Add the unvisited path imports to the queue.
			let iter = analysis.imports.iter().filter_map(|import| {
				if let tg::import::Specifier::Path(path) = &import.specifier {
					Some((path.clone(), import.r#type))
				} else {
					None
				}
			});
			for entry in iter {
				if !visited.contains(&entry) && !queue.contains(&entry) {
					queue.push_back(entry);
				}
			}
		}

		Ok(dependencies.into_iter().collect())
	}
}
