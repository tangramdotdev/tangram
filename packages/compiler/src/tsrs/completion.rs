use {
	super::{Project, position, range},
	crate::completion,
	lsp_types as lsp,
	tangram_client::prelude::*,
	tsrs_lsproto::{self as native, json::Json as _},
};

impl Project {
	pub(super) fn completion(&self, arg: &completion::Arg) -> tg::Result<completion::Output> {
		self.with_language_service_with_auto_imports(&arg.module, |context, service| {
			let output = service
				.provide_completion(
					context,
					&self.uri(&arg.module),
					position(arg.position),
					None,
				)
				.map_err(|error| tg::error!(!error, "failed to get the tsrs completions"))?;
			let items = output.items.or_else(|| output.list.map(|list| list.items));
			let entries = items
				.map(|items| {
					items
						.into_iter()
						.map(completion_entry)
						.collect::<tg::Result<_>>()
				})
				.transpose()?;
			let output = completion::Output { entries };

			Ok(output)
		})
	}

	pub(super) fn completion_resolve(
		&self,
		arg: completion::ResolveArg,
	) -> tg::Result<completion::ResolveOutput> {
		self.with_language_service_with_auto_imports(&arg.module, |context, service| {
			// Restore the native completion metadata carried by Tangram's typed request.
			let item = if let Some(data) = arg.data {
				let text = serde_json::to_string(&data).map_err(|error| {
					tg::error!(!error, "failed to encode the completion metadata")
				})?;
				let data = tsrs_core::json::unmarshal(&text)
					.map_err(|error| tg::error!(%error, "invalid tsrs completion metadata"))?;
				Some(
					native::CompletionItem::from_json(&data)
						.map_err(|error| tg::error!(!error, "invalid tsrs completion item"))?,
				)
			} else {
				let output = service
					.provide_completion(
						context,
						&self.uri(&arg.module),
						position(arg.position),
						None,
					)
					.map_err(|error| tg::error!(!error, "failed to get the tsrs completions"))?;
				output
					.items
					.or_else(|| output.list.map(|list| list.items))
					.unwrap_or_default()
					.into_iter()
					.find(|item| {
						item.label == arg.name
							&& arg.source.as_ref().is_none_or(|source| {
								item.data
									.as_ref()
									.is_some_and(|data| &data.source == source)
							})
					})
			};
			let Some(item) = item else {
				return Ok(completion::ResolveOutput { entry: None });
			};

			// Resolve the item through the native language service.
			let data = item.data.clone();
			let item = service
				.resolve_completion_item(context, item, data.as_ref())
				.map_err(|error| tg::error!(!error, "failed to resolve the tsrs completion"))?;
			let kind_modifiers = completion_kind_modifiers(&item);
			let additional_text_edits = item.additional_text_edits.map(|edits| {
				edits
					.into_iter()
					.map(|edit| completion::TextEdit {
						new_text: edit.new_text,
						range: range(edit.range),
					})
					.collect()
			});
			let detail = item.detail;
			let documentation = item.documentation.and_then(|value| {
				value
					.string
					.or_else(|| value.markup_content.map(|value| value.value))
			});
			let kind = completion_kind(item.kind).to_owned();
			let entry = Some(completion::EntryDetails {
				additional_text_edits,
				detail,
				documentation,
				kind,
				kind_modifiers,
			});
			let output = completion::ResolveOutput { entry };

			Ok(output)
		})
	}
}

fn completion_entry(item: native::CompletionItem) -> tg::Result<completion::Entry> {
	// Preserve the native item so completion resolution retains its source and auto-import metadata.
	let text = tsrs_core::json::marshal(&item.to_json())
		.map_err(|error| tg::error!(%error, "failed to encode the tsrs completion item"))?;
	let data = Some(
		serde_json::from_str(&text)
			.map_err(|error| tg::error!(!error, "failed to decode the tsrs completion item"))?,
	);
	let source = item
		.data
		.as_ref()
		.map(|data| data.source.clone())
		.filter(|source| !source.is_empty());
	let kind = completion_kind(item.kind).to_owned();
	let kind_modifiers = completion_kind_modifiers(&item);
	let is_snippet = item
		.insert_text_format
		.map(|format| format == native::InsertTextFormat::Snippet);
	let label_details = item.label_details.map(|details| completion::LabelDetails {
		description: details.description,
		detail: details.detail,
	});
	let sort_text = item.sort_text.unwrap_or_else(|| item.label.clone());
	let text_edit = item.text_edit.and_then(|edit| {
		if let Some(edit) = edit.text_edit {
			let edit = lsp::TextEdit {
				new_text: edit.new_text,
				range: range(edit.range).into(),
			};
			Some(lsp::CompletionTextEdit::Edit(edit))
		} else {
			edit.insert_replace_edit.map(|edit| {
				let edit = lsp::InsertReplaceEdit {
					insert: range(edit.insert).into(),
					new_text: edit.new_text,
					replace: range(edit.replace).into(),
				};
				lsp::CompletionTextEdit::InsertAndReplace(edit)
			})
		}
	});
	let entry = completion::Entry {
		commit_characters: item.commit_characters,
		data,
		filter_text: item.filter_text,
		insert_text: item.insert_text,
		is_snippet,
		kind,
		kind_modifiers,
		label_details,
		name: item.label,
		sort_text,
		source,
		text_edit,
	};

	Ok(entry)
}

fn completion_kind_modifiers(item: &native::CompletionItem) -> Option<String> {
	let deprecated = item.deprecated.unwrap_or(false)
		|| item
			.tags
			.as_ref()
			.is_some_and(|tags| tags.contains(&native::CompletionItemTag::Deprecated));
	deprecated.then(|| "deprecated".to_owned())
}

fn completion_kind(kind: Option<native::CompletionItemKind>) -> &'static str {
	match kind {
		Some(native::CompletionItemKind::Class | native::CompletionItemKind::Struct) => "class",
		Some(native::CompletionItemKind::Constant) => "const",
		Some(native::CompletionItemKind::Constructor) => "constructor",
		Some(native::CompletionItemKind::Enum) => "enum",
		Some(native::CompletionItemKind::EnumMember) => "enum member",
		Some(native::CompletionItemKind::Field | native::CompletionItemKind::Property) => {
			"property"
		},
		Some(native::CompletionItemKind::Folder) => "directory",
		Some(native::CompletionItemKind::Function) => "function",
		Some(native::CompletionItemKind::Interface) => "interface",
		Some(native::CompletionItemKind::Keyword) => "keyword",
		Some(native::CompletionItemKind::Method) => "method",
		Some(native::CompletionItemKind::Module) => "module",
		Some(native::CompletionItemKind::Reference) => "alias",
		Some(native::CompletionItemKind::TypeParameter) => "type parameter",
		Some(native::CompletionItemKind::Variable) => "var",
		_ => "",
	}
}
