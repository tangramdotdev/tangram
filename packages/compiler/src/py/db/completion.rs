use {
	super::Database, ruff_db::files::File, tangram_client::prelude::*, ty_python_semantic::Db as _,
};

impl Database {
	pub(super) fn completion(
		&self,
		file: File,
		position: tg::Position,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::completion::Response> {
		let offset = self.offset(file, position, encoding)?;
		let file = self.program_file(file);
		let environment = ty_python_semantic::ProgramEnvironment::from_file(file);
		// Import edits require Tangram dependency declarations rather than ty's filesystem module names.
		let settings = ty_ide::CompletionSettings {
			auto_import: false,
			complete_function_parentheses: false,
		};
		let capabilities = ty_ide::CompletionCapabilities::default();
		let entries = ty_ide::completion(self, &settings, capabilities, file, offset)
			.into_iter()
			.enumerate()
			.map(|(index, mut completion)| {
				let kind = completion.kind.map_or("", kind).to_owned();
				let detail = completion
					.ty
					.map(|ty| ty.display(self, &environment).to_string());
				let documentation = completion
					.documentation
					.take()
					.map(|doc| doc.render_plaintext());
				let details = crate::completion::EntryDetails {
					detail,
					documentation,
					kind: kind.clone(),
					kind_modifiers: None,
				};
				let data = serde_json::to_value(details).map_err(|error| {
					tg::error!(!error, "failed to serialize the Python completion")
				})?;
				let entry = crate::completion::Entry {
					commit_characters: None,
					data: Some(data),
					filter_text: Some(completion.name.to_string()),
					insert_text: Some(
						completion
							.insert
							.as_deref()
							.unwrap_or(&completion.name)
							.to_owned(),
					),
					is_snippet: Some(false),
					kind,
					kind_modifiers: None,
					label_details: None,
					name: completion.label().to_owned(),
					sort_text: format!("{index:010}"),
					source: None,
				};
				Ok(entry)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::completion::Response {
			entries: Some(entries),
		})
	}
}

fn kind(kind: ty_ide::CompletionKind) -> &'static str {
	use ty_ide::CompletionKind as Kind;
	match kind {
		Kind::Class => "class",
		Kind::Constant => "const",
		Kind::Constructor => "constructor",
		Kind::Enum => "enum",
		Kind::EnumMember => "enum member",
		Kind::Field | Kind::Property => "property",
		Kind::File => "script",
		Kind::Folder => "directory",
		Kind::Function => "function",
		Kind::Interface => "interface",
		Kind::Keyword => "keyword",
		Kind::Method => "method",
		Kind::Module => "module",
		Kind::TypeParameter => "type parameter",
		Kind::Variable => "var",
		Kind::Color
		| Kind::Event
		| Kind::Operator
		| Kind::Reference
		| Kind::Snippet
		| Kind::Struct
		| Kind::Text
		| Kind::Unit
		| Kind::Value => "",
	}
}
