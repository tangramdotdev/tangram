use {
	super::Database,
	ruff_db::{files::File, source::source_text},
	tangram_client::prelude::*,
	ty_python_semantic::Db as _,
};

impl Database {
	pub(super) fn semantic_tokens(
		&self,
		file: File,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::semantic_tokens::Response> {
		let text = source_text(self, file);
		let mut tokens = Vec::new();
		for token in ty_ide::semantic_tokens(self, self.program_file(file), None).iter() {
			let token_type = u32::try_from(
				crate::semantic_tokens::TYPES
					.iter()
					.position(|name| *name == token.token_type.as_lsp_concept())
					.unwrap(),
			)
			.unwrap();
			let mut token_modifiers_bitset = 0;
			for (modifier, name) in [
				(ty_ide::SemanticTokenModifier::DEFINITION, "definition"),
				(ty_ide::SemanticTokenModifier::READONLY, "readonly"),
				(ty_ide::SemanticTokenModifier::ASYNC, "async"),
				(
					ty_ide::SemanticTokenModifier::DOCUMENTATION,
					"documentation",
				),
			] {
				if token.modifiers.contains(modifier) {
					let index = crate::semantic_tokens::MODIFIERS
						.iter()
						.position(|value| *value == name)
						.unwrap();
					token_modifiers_bitset |= 1 << index;
				}
			}
			// LSP clients need single-line tokens unless multiline support was negotiated.
			let mut offset = usize::from(token.range.start());
			for line in text[offset..usize::from(token.range.end())].split_inclusive('\n') {
				let content = line.trim_end_matches(['\r', '\n']);
				if !content.is_empty() {
					let range = tg::Range::try_from_byte_range_in_string(
						&text,
						offset..offset + content.len(),
						encoding,
					)
					.ok_or_else(|| tg::error!("invalid Python token range"))?;
					tokens.push(crate::semantic_tokens::Token {
						length: range.end.character - range.start.character,
						line: range.start.line,
						start: range.start.character,
						token_modifiers_bitset,
						token_type,
					});
				}
				offset += line.len();
			}
		}
		Ok(crate::semantic_tokens::Response {
			tokens: Some(tokens),
		})
	}
}
