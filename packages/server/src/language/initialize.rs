use super::Server;
use lsp_types as lsp;
use tangram_client as tg;

impl Server {
	pub(super) async fn handle_initialize_request(
		&self,
		params: lsp::InitializeParams,
	) -> tg::Result<lsp::InitializeResult> {
		let workspaces = params
			.workspace_folders
			.into_iter()
			.flatten()
			.map(|folder| folder.uri)
			.collect();
		self.update_workspaces(workspaces, Vec::new()).await.ok();

		let output = lsp::InitializeResult {
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

		Ok(output)
	}
}
