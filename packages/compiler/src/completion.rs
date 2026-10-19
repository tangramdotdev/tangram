use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug)]
pub struct Output {
	pub entries: Option<Vec<Entry>>,
}

#[derive(Debug)]
pub struct ResolveArg {
	pub module: tg::module::Data,
	pub position: tg::Position,
	pub name: String,
	pub source: Option<String>,
	pub data: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct ResolveOutput {
	pub entry: Option<EntryDetails>,
}

#[derive(Debug)]
pub struct Entry {
	pub commit_characters: Option<Vec<String>>,
	pub data: Option<serde_json::Value>,
	pub filter_text: Option<String>,
	pub insert_text: Option<String>,
	pub is_snippet: Option<bool>,
	pub kind: String,
	pub kind_modifiers: Option<String>,
	pub label_details: Option<LabelDetails>,
	pub name: String,
	pub sort_text: String,
	pub source: Option<String>,
	pub text_edit: Option<lsp::CompletionTextEdit>,
}

#[derive(Debug)]
pub struct LabelDetails {
	pub detail: Option<String>,
	pub description: Option<String>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryDetails {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub additional_text_edits: Option<Vec<TextEdit>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub detail: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub documentation: Option<String>,

	pub kind: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind_modifiers: Option<String>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TextEdit {
	pub new_text: String,
	pub range: tg::Range,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ResolveData {
	module: tg::module::Data,
	position: tg::Position,
	name: String,
	source: Option<String>,
	data: Option<serde_json::Value>,
}

impl Compiler {
	pub async fn completion(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<Entry>>> {
		if self.is_generated_module(module) {
			return Ok(None);
		}

		// Create the arg.
		let arg = Arg {
			module: module.clone(),
			position,
		};

		// Get the output.
		let output = self.backend(module.kind).completion(self, arg).await?;

		Ok(output.entries)
	}

	pub async fn completion_resolve(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
		name: String,
		source: Option<String>,
		data: Option<serde_json::Value>,
	) -> tg::Result<Option<EntryDetails>> {
		if self.is_generated_module(module) {
			return Ok(None);
		}

		// Create the arg.
		let arg = ResolveArg {
			module: module.clone(),
			position,
			name,
			source,
			data,
		};

		// Get the output.
		let output = self
			.backend(module.kind)
			.completion_resolve(self, arg)
			.await?;

		Ok(output.entry)
	}
}

impl Compiler {
	pub(super) async fn handle_completion_request(
		&self,
		params: lsp::CompletionParams,
	) -> tg::Result<Option<lsp::CompletionResponse>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position.position;

		// Get the completion entries.
		let entries = self.completion(&module, position.into()).await?;
		let Some(entries) = entries else {
			return Ok(None);
		};

		// Convert the completion entries.
		let entries = entries
			.into_iter()
			.map(|entry| {
				let data = ResolveData {
					module: module.clone(),
					position: position.into(),
					name: entry.name.clone(),
					source: entry.source.clone(),
					data: entry.data,
				};
				let kind = Some(completion_item_kind_for_script_element_kind(&entry.kind));
				let tags = is_kind_modifiers_deprecated(entry.kind_modifiers.as_deref())
					.then_some(vec![lsp::CompletionItemTag::DEPRECATED]);
				let label_details =
					entry
						.label_details
						.map(|label_details| lsp::CompletionItemLabelDetails {
							detail: label_details.detail,
							description: label_details.description,
						});
				lsp::CompletionItem {
					label: entry.name,
					kind,
					tags,
					sort_text: Some(entry.sort_text),
					filter_text: entry.filter_text,
					insert_text: entry.insert_text,
					text_edit: entry.text_edit,
					insert_text_format: if entry.is_snippet.unwrap_or(false) {
						Some(lsp::InsertTextFormat::SNIPPET)
					} else {
						None
					},
					commit_characters: entry.commit_characters,
					label_details,
					data: serde_json::to_value(data).ok(),
					..Default::default()
				}
			})
			.collect();

		let response = if module.kind == tg::module::Kind::Python {
			// ty filters completions against the typed prefix, so further typing needs a new query.
			lsp::CompletionResponse::List(lsp::CompletionList {
				is_incomplete: true,
				items: entries,
			})
		} else {
			lsp::CompletionResponse::Array(entries)
		};
		Ok(Some(response))
	}

	pub(super) async fn handle_completion_item_resolve_request(
		&self,
		mut item: lsp::CompletionItem,
	) -> tg::Result<lsp::CompletionItem> {
		// Deserialize the completion resolve data.
		let Some(data) = item.data.clone() else {
			return Ok(item);
		};
		let Ok(data) = serde_json::from_value::<ResolveData>(data) else {
			return Ok(item);
		};

		// Resolve the completion details.
		let entry = self
			.completion_resolve(
				&data.module,
				data.position,
				data.name,
				data.source,
				data.data,
			)
			.await?;
		let Some(entry) = entry else {
			return Ok(item);
		};

		if item.kind.is_none() {
			item.kind = Some(completion_item_kind_for_script_element_kind(&entry.kind));
		}
		if item.tags.is_none() && is_kind_modifiers_deprecated(entry.kind_modifiers.as_deref()) {
			item.tags = Some(vec![lsp::CompletionItemTag::DEPRECATED]);
		}
		item.additional_text_edits = entry.additional_text_edits.map(|edits| {
			edits
				.into_iter()
				.map(|edit| lsp::TextEdit {
					new_text: edit.new_text,
					range: edit.range.into(),
				})
				.collect()
		});
		item.detail = entry.detail;
		item.documentation = entry.documentation.map(lsp::Documentation::String);

		Ok(item)
	}
}

fn completion_item_kind_for_script_element_kind(kind: &str) -> lsp::CompletionItemKind {
	match kind {
		"class" | "local class" => lsp::CompletionItemKind::CLASS,
		"enum" => lsp::CompletionItemKind::ENUM,
		"enum member" => lsp::CompletionItemKind::ENUM_MEMBER,
		"interface" => lsp::CompletionItemKind::INTERFACE,
		"module" | "external module" => lsp::CompletionItemKind::MODULE,
		"type" | "type parameter" | "primitive type" => lsp::CompletionItemKind::TYPE_PARAMETER,
		"parameter" | "var" | "local var" | "let" | "using" | "await using" => {
			lsp::CompletionItemKind::VARIABLE
		},
		"const" => lsp::CompletionItemKind::CONSTANT,
		"property" | "accessor" | "getter" | "setter" => lsp::CompletionItemKind::PROPERTY,
		"method" => lsp::CompletionItemKind::METHOD,
		"function" | "local function" => lsp::CompletionItemKind::FUNCTION,
		"constructor" | "construct" => lsp::CompletionItemKind::CONSTRUCTOR,
		"keyword" => lsp::CompletionItemKind::KEYWORD,
		"directory" => lsp::CompletionItemKind::FOLDER,
		"alias" => lsp::CompletionItemKind::REFERENCE,
		_ => lsp::CompletionItemKind::TEXT,
	}
}

fn is_kind_modifiers_deprecated(kind_modifiers: Option<&str>) -> bool {
	let Some(kind_modifiers) = kind_modifiers else {
		return false;
	};
	kind_modifiers
		.split(',')
		.any(|modifier| modifier.trim() == "deprecated")
}
