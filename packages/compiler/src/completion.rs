use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub entries: Option<Vec<Entry>>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveRequest {
	pub module: tg::module::Data,
	pub position: tg::Position,
	pub name: String,
	pub source: Option<String>,
	pub data: Option<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ResolveResponse {
	pub entry: Option<EntryDetails>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
	pub name: String,
	pub kind: String,
	pub kind_modifiers: Option<String>,
	pub sort_text: String,
	pub insert_text: Option<String>,
	pub filter_text: Option<String>,
	pub is_snippet: Option<bool>,
	pub source: Option<String>,
	pub commit_characters: Option<Vec<String>>,
	pub data: Option<serde_json::Value>,
	pub label_details: Option<LabelDetails>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LabelDetails {
	pub detail: Option<String>,
	pub description: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryDetails {
	pub kind: String,
	pub kind_modifiers: Option<String>,
	pub detail: Option<String>,
	pub documentation: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
		// Create the request.
		let request = super::Request::Completion(Request {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::Completion(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.entries)
	}

	pub async fn completion_resolve(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
		name: String,
		source: Option<String>,
		data: Option<serde_json::Value>,
	) -> tg::Result<Option<EntryDetails>> {
		// Create the request.
		let request = super::Request::CompletionResolve(ResolveRequest {
			module: module.clone(),
			position,
			name,
			source,
			data,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::CompletionResolve(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.entry)
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

		Ok(Some(lsp::CompletionResponse::Array(entries)))
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
