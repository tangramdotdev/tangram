use crate::Server;
use async_recursion::async_recursion;
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{error, Result};

/// The possible file names of the root module in a package.
pub const ROOT_MODULE_FILE_NAMES: &[&str] =
	&["tangram.js", "tangram.tg.js", "tangram.tg.ts", "tangram.ts"];

impl Server {
	pub async fn format_package(&self, dependency: &tg::Dependency) -> Result<()> {
		let path = dependency
			.path
			.as_ref()
			.ok_or_else(|| error!(%dependency, "expected a path"))?;

		// Find the root module path.
		let root_module_path = 'a: {
			for file_name in ROOT_MODULE_FILE_NAMES {
				let path = path.clone().join(*file_name);
				let exists = tokio::fs::try_exists(&path).await.map_err(
					|error| error!(source = error, %path, "failed to check if file exists"),
				)?;
				if exists {
					break 'a path;
				}
			}
			return Err(error!(%path, "expected a root module"));
		};
		let mut visited_module_paths = im::HashSet::default();
		self.format_module(root_module_path, &mut visited_module_paths)
			.await?;
		Ok(())
	}

	#[async_recursion]
	async fn format_module(
		&self,
		module_path: tg::Path,
		visited_module_paths: &mut im::HashSet<PathBuf, fnv::FnvBuildHasher>,
	) -> Result<()> {
		let module_absolute_path = tokio::fs::canonicalize(&module_path).await.map_err(
			|error| error!(source = error, %module_path, "failed to canonicalize module path"),
		)?;

		if visited_module_paths.contains(&module_absolute_path) {
			return Ok(());
		}
		visited_module_paths.insert(module_absolute_path.clone());

		// Get the module's text.
		let text = tokio::fs::read_to_string(&module_absolute_path)
			.await
			.map_err(|source| error!(!source, "failed to read the module"))?;

		// Format the module's text.
		let text = self
			.format(text)
			.await
			.map_err(|source| error!(!source, %module_path, "failed to format module"))?;

		// Write the new text back.
		tokio::fs::write(&module_absolute_path, text.as_bytes())
			.await
			.map_err(
				|error| error!(source = error, %module_path, "failed to write formatted module"),
			)?;

		// Try to analyze the module. We don't want to return an error early in case the module contains syntax errors.
		let Ok(analysis) = crate::language::Server::analyze_module(text) else {
			return Ok(());
		};

		for import in analysis.imports {
			if let tg::Import::Module(module) = import {
				let module_path = module_path.clone().parent().normalize().join(module);
				let exists = tokio::fs::try_exists(&module_path).await.map_err(
					|error| error!(source = error, %module_path, "failed to check if module exists"),
				)?;
				if exists {
					self.format_module(module_path, visited_module_paths)
						.await?;
				}
			}
		}

		Ok(())
	}
}
