use super::{Result, Server};
use lsp_types as lsp;

impl Server {
	pub(super) async fn handle_initialize_request(
		&self,
		params: lsp::InitializeParams,
	) -> Result<lsp::InitializeResult> {
		let supports_workspace_folders = params
			.capabilities
			.workspace
			.as_ref()
			.and_then(|ws| ws.workspace_folders)
			.unwrap_or(false);

		// Collect the workspace folders. We swallow any errors here to avoid crashing the server at initialization.
		let added = if supports_workspace_folders {
			params
				.workspace_folders
				.into_iter()
				.flatten()
				.map(|folder| folder.uri)
				.collect()
		} else {
			params.root_uri.into_iter().collect()
		};

		self.update_workspaces(added, Vec::new()).await.ok();

		let capabilities = lsp::InitializeResult {
			capabilities: lsp::ServerCapabilities {
				text_document_sync: Some(lsp::TextDocumentSyncCapability::Options(
					lsp::TextDocumentSyncOptions {
						open_close: Some(true),
						change: Some(lsp::TextDocumentSyncKind::INCREMENTAL),
						..Default::default()
					},
				)),
				hover_provider: Some(lsp::HoverProviderCapability::Simple(true)),
				completion_provider: Some(lsp::CompletionOptions::default()),
				definition_provider: Some(lsp::OneOf::Left(true)),
				references_provider: Some(lsp::OneOf::Left(true)),
				document_formatting_provider: Some(lsp::OneOf::Left(true)),
				document_symbol_provider: Some(lsp::OneOf::Left(true)),
				rename_provider: Some(lsp::OneOf::Left(true)),
				workspace: Some(lsp::WorkspaceServerCapabilities {
					workspace_folders: Some(lsp::WorkspaceFoldersServerCapabilities {
						supported: Some(true),
						change_notifications: Some(lsp::OneOf::Left(true)),
					}),
					..Default::default()
				}),
				..Default::default()
			},
			..Default::default()
		};
		Ok(capabilities)
	}
}
