use super::Compiler;
use lsp_types as lsp;
use std::path::PathBuf;
use tangram_client as tg;

impl Compiler {
	pub(crate) async fn update_workspaces(
		&self,
		added: Vec<lsp::Uri>,
		removed: Vec<lsp::Uri>,
	) -> tg::Result<()> {
		// Get the state.
		let mut workspaces = self.workspaces.write().await;

		// Add the specified workspaces.
		for uri in added {
			let path = match uri.scheme().unwrap().as_str() {
				"file" => PathBuf::from(uri.path().as_str()),
				scheme => {
					return Err(tg::error!(%scheme, "invalid URI for workspace folder"));
				},
			};
			workspaces.insert(path);
		}

		// Remove the specified workspaces.
		for uri in removed {
			let path = match uri.scheme().unwrap().as_str() {
				"file" => PathBuf::from(uri.path().as_str()),
				scheme => {
					return Err(tg::error!(%scheme, "invalid URI for workspace folder"));
				},
			};
			workspaces.remove(&path);
		}

		Ok(())
	}
}

impl Compiler {
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

		Ok(())
	}
}
