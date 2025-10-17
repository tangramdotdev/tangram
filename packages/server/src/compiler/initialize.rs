use {super::Compiler, lsp_types as lsp, tangram_client as tg};

impl Compiler {
	pub(super) async fn handle_initialize_request(
		&self,
		params: lsp::InitializeParams,
	) -> tg::Result<lsp::InitializeResult> {
		let workspaces = params
			.workspace_folders
			.into_iter()
			.flatten()
			.map(|workspace_folder| workspace_folder.uri)
			.collect();
		self.update_workspaces(workspaces, Vec::new()).await.ok();

		let output = lsp::InitializeResult {
			capabilities: lsp::ServerCapabilities {
				position_encoding: Some(lsp::PositionEncodingKind::UTF8),
				completion_provider: Some(lsp::CompletionOptions::default()),
				definition_provider: Some(lsp::OneOf::Left(true)),
				diagnostic_provider: Some(lsp::DiagnosticServerCapabilities::Options(
					lsp::DiagnosticOptions {
						identifier: Some("tangram".to_owned()),
						inter_file_dependencies: true,
						..Default::default()
					},
				)),
				document_formatting_provider: Some(lsp::OneOf::Left(true)),
				document_symbol_provider: Some(lsp::OneOf::Left(true)),
				hover_provider: Some(lsp::HoverProviderCapability::Simple(true)),
				references_provider: Some(lsp::OneOf::Left(true)),
				rename_provider: Some(lsp::OneOf::Left(true)),
				type_definition_provider: Some(lsp::TypeDefinitionProviderCapability::Simple(true)),
				text_document_sync: Some(lsp::TextDocumentSyncCapability::Options(
					lsp::TextDocumentSyncOptions {
						open_close: Some(true),
						change: Some(lsp::TextDocumentSyncKind::INCREMENTAL),
						..Default::default()
					},
				)),
				workspace: Some(lsp::WorkspaceServerCapabilities {
					workspace_folders: Some(lsp::WorkspaceFoldersServerCapabilities {
						supported: Some(true),
						change_notifications: Some(lsp::OneOf::Left(true)),
					}),
					..Default::default()
				}),
				..Default::default()
			},
			server_info: Some(lsp::ServerInfo {
				name: "tangram".to_owned(),
				version: Some(self.server.version.clone()),
			}),
		};

		Ok(output)
	}
}
