use crate::{compiler::Compiler, Server};
use std::collections::HashSet;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn format_package(&self, arg: tg::package::format::Arg) -> tg::Result<()> {
		// Get the root module path.
		let root_module_file_name =
			tg::package::try_get_root_module_file_name_for_package_path(arg.path.as_ref())
				.await?
				.ok_or_else(|| tg::error!("failed to find the root module"))?;
		let path = arg.path.join(root_module_file_name);

		// Format the modules recursively.
		let mut visited = HashSet::default();
		self.format_module(path, &mut visited).await?;

		Ok(())
	}

	async fn format_module(
		&self,
		path: tg::Path,
		visited: &mut HashSet<tg::Path, fnv::FnvBuildHasher>,
	) -> tg::Result<()> {
		if visited.contains(&path) {
			return Ok(());
		}
		visited.insert(path.clone());

		// Get the text.
		let text = tokio::fs::read_to_string(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the module"))?;

		// Format the text.
		let text = self
			.format(text)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to format module"))?;

		// Write the text.
		tokio::fs::write(&path, text.as_bytes())
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to write formatted module"))?;

		// Attempt to analyze the module.
		let Ok(analysis) = Compiler::analyze_module(text) else {
			return Ok(());
		};

		for import in analysis.imports {
			if let tg::reference::Path::Path(path_) = &import.reference.path() {
				let path = path.clone().parent().normalize().join(path_.clone());
				let exists = tokio::fs::try_exists(&path).await.map_err(
					|source| tg::error!(!source, %path, "failed to check if module exists"),
				)?;
				if exists {
					Box::pin(self.format_module(path, visited)).await?;
				}
			}
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_format_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.format_package(arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
