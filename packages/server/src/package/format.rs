use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};

impl Server {
	pub async fn format_package(&self, dependency: &tg::Dependency) -> tg::Result<()> {
		// Get the path from the dependency.
		let path = dependency
			.path
			.as_ref()
			.ok_or_else(|| tg::error!(%dependency, "expected the dependency to have a path"))?;

		// Get the root module path.
		let root_module_path = tg::package::get_root_module_path_for_path(path.as_ref()).await?;

		// Format the modules recursively.
		let mut visited_module_paths = im::HashSet::default();
		self.format_module(root_module_path, &mut visited_module_paths)
			.await?;

		Ok(())
	}

	async fn format_module(
		&self,
		module_path: tg::Path,
		visited_module_paths: &mut im::HashSet<tg::Path, fnv::FnvBuildHasher>,
	) -> tg::Result<()> {
		if visited_module_paths.contains(&module_path) {
			return Ok(());
		}
		visited_module_paths.insert(module_path.clone());

		// Get the module's text.
		let text = tokio::fs::read_to_string(&module_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the module"))?;

		// Format the module's text.
		let text = self
			.format(text)
			.await
			.map_err(|source| tg::error!(!source, %module_path, "failed to format module"))?;

		// Write the new text back.
		tokio::fs::write(&module_path, text.as_bytes())
			.await
			.map_err(
				|source| tg::error!(!source, %module_path, "failed to write formatted module"),
			)?;

		// Attempt to analyze the module.
		let Ok(analysis) = crate::language::Server::analyze_module(text) else {
			return Ok(());
		};

		for import in analysis.imports {
			if let tg::Import::Module(module) = import {
				let module_path = module_path.clone().parent().normalize().join(module);
				let exists = tokio::fs::try_exists(&module_path).await.map_err(
					|source| tg::error!(!source, %module_path, "failed to check if module exists"),
				)?;
				if exists {
					Box::pin(self.format_module(module_path, visited_module_paths)).await?;
				}
			}
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_format_package_request<H>(
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

		// Format the package.
		handle.format_package(&dependency).await?;

		// Create the response.
		let response = http::Response::builder().body(Outgoing::empty()).unwrap();

		Ok(response)
	}
}
