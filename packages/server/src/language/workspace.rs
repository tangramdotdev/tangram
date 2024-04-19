use super::Server;
use lsp_types as lsp;
use std::path::PathBuf;
use tangram_client as tg;

impl Server {
	pub(crate) async fn update_workspaces(
		&self,
		added: Vec<lsp::Url>,
		removed: Vec<lsp::Url>,
	) -> tg::Result<()> {
		// Get the state.
		let mut workspaces = self.workspaces.write().await;

		// Add the specified workspaces.
		for uri in added {
			let package_path = match uri.scheme() {
				"file" => PathBuf::from(uri.path()),
				scheme => return Err(tg::error!(%scheme, "invalid URI for workspace folder")),
			};
			workspaces.insert(package_path);
		}

		// Remove the specified workspaces.
		for uri in removed {
			let package_path = match uri.scheme() {
				"file" => PathBuf::from(uri.path()),
				scheme => return Err(tg::error!(%scheme, "invalid URI for workspace folder")),
			};
			workspaces.remove(&package_path);
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_did_change_workspace_folders(
		&self,
		params: lsp::DidChangeWorkspaceFoldersParams,
	) -> tg::Result<()> {
		// Collect the added and removed workspaces.
		let added = params
			.event
			.added
			.into_iter()
			.map(|folder| folder.uri)
			.collect();
		let removed = params
			.event
			.removed
			.into_iter()
			.map(|folder| folder.uri)
			.collect();

		// Update the workspaces.
		self.update_workspaces(added, removed).await?;

		// Update the diagnostics.
		self.update_diagnostics().await?;

		Ok(())
	}
}
