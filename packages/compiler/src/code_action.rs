use {super::Compiler, lsp_types as lsp, std::collections::HashMap, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub range: tg::Range,
	pub only: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct Output {
	pub actions: Option<Vec<Action>>,
}

#[derive(Debug)]
pub struct Action {
	pub title: String,
	pub kind: Option<String>,
	pub edits: Option<Vec<Edit>>,
}

#[derive(Debug)]
pub struct Edit {
	pub module: tg::module::Data,
	pub new_text: String,
	pub range: tg::Range,
}

impl Compiler {
	pub async fn code_actions(
		&self,
		module: &tg::module::Data,
		range: tg::Range,
		only: Option<Vec<String>>,
	) -> tg::Result<Option<Vec<Action>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			range,
			only,
		};

		// Get the output.
		let output = self.backend(module.kind).code_action(self, arg).await?;

		// Exclude actions that would apply positions in bindings to the module that they were generated from.
		let actions = output.actions.map(|actions| {
			actions
				.into_iter()
				.filter(|action| {
					action.edits.as_ref().is_none_or(|edits| {
						edits
							.iter()
							.all(|edit| Self::is_editable(module.kind, &edit.module))
					})
				})
				.collect::<Vec<_>>()
		});
		let actions = actions.filter(|actions| !actions.is_empty());

		Ok(actions)
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
