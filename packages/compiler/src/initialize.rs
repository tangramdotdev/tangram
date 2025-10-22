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

		// Negotiate position encoding with the client.
		let position_encoding = params
			.capabilities
			.general
			.and_then(|general| general.position_encodings)
			.and_then(|encodings| {
				if encodings.contains(&lsp::PositionEncodingKind::UTF8) {
					Some(lsp::PositionEncodingKind::UTF8)
				} else if encodings.contains(&lsp::PositionEncodingKind::UTF16) {
					Some(lsp::PositionEncodingKind::UTF16)
				} else {
					None
				}
			})
			.unwrap_or(lsp::PositionEncodingKind::UTF16);

		// Store the negotiated encoding.
		*self.position_encoding.write().unwrap() =
			if position_encoding == lsp::PositionEncodingKind::UTF8 {
				tg::position::Encoding::Utf8
			} else if position_encoding == lsp::PositionEncodingKind::UTF16 {
				tg::position::Encoding::Utf16
			} else {
				unreachable!()
			};

		let output = lsp::InitializeResult {
			capabilities: lsp::ServerCapabilities {
				position_encoding: Some(position_encoding),
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
						save: Some(lsp::TextDocumentSyncSaveOptions::Supported(true)),
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
				version: Some(self.version.clone()),
			}),
		};

		Ok(output)
	}
}
