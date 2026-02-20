use {super::Compiler, lsp_types as lsp, std::collections::HashMap, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub range: tg::Range,
	pub only: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub actions: Option<Vec<Action>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Action {
	pub title: String,
	pub kind: Option<String>,
	pub edits: Option<Vec<Edit>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Edit {
	pub module: tg::module::Data,
	pub range: tg::Range,
	pub new_text: String,
}

impl Compiler {
	pub async fn code_actions(
		&self,
		module: &tg::module::Data,
		range: tg::Range,
		only: Option<Vec<String>>,
	) -> tg::Result<Option<Vec<Action>>> {
		// Create the request.
		let request = super::Request::CodeAction(Request {
			module: module.clone(),
			range,
			only,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::CodeAction(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.actions)
	}
}

impl Compiler {
	pub(crate) async fn handle_code_action_request(
		&self,
		params: lsp::CodeActionParams,
	) -> tg::Result<Option<lsp::CodeActionResponse>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the range and kinds for the request.
		let range = params.range;
		let only = params.context.only.map(|only| {
			only.into_iter()
				.map(|kind| kind.as_str().to_owned())
				.collect()
		});

		// Get the code actions.
		let actions = self.code_actions(&module, range.into(), only).await?;
		let Some(actions) = actions else {
			return Ok(None);
		};

		// Convert the actions.
		let mut converted_actions = Vec::new();
		for action in actions {
			let edit = if let Some(edits) = action.edits {
				Some(self.workspace_edit_from_edits(edits).await?)
			} else {
				None
			};
			let action = lsp::CodeAction {
				title: action.title,
				kind: action.kind.map(Into::into),
				diagnostics: None,
				edit,
				command: None,
				is_preferred: None,
				disabled: None,
				data: None,
			};
			converted_actions.push(lsp::CodeActionOrCommand::CodeAction(action));
		}

		Ok(Some(converted_actions))
	}

	pub(crate) async fn handle_code_action_resolve_request(
		&self,
		action: lsp::CodeAction,
	) -> tg::Result<lsp::CodeAction> {
		let _ = self;
		Ok(action)
	}

	async fn workspace_edit_from_edits(&self, edits: Vec<Edit>) -> tg::Result<lsp::WorkspaceEdit> {
		#[expect(clippy::mutable_key_type)]
		let mut changes = HashMap::<lsp::Uri, Vec<lsp::TextEdit>>::new();

		for edit in edits {
			let uri = self.lsp_uri_for_module(&edit.module).await?;
			changes.entry(uri).or_default().push(lsp::TextEdit {
				range: edit.range.into(),
				new_text: edit.new_text,
			});
		}

		let changes = if changes.is_empty() {
			None
		} else {
			Some(changes)
		};

		let edit = lsp::WorkspaceEdit {
			changes,
			document_changes: None,
			change_annotations: None,
		};

		Ok(edit)
	}
}
