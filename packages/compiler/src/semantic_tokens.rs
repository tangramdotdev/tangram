use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug, serde::Serialize)]
pub struct Request {
	pub module: tg::module::Data,
}

#[derive(Debug, serde::Deserialize)]
pub struct Response {
	pub tokens: Option<Vec<Token>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Token {
	pub line: u32,
	pub start: u32,
	pub length: u32,
	pub token_type: u32,
	pub token_modifiers_bitset: u32,
}

impl Compiler {
	pub async fn semantic_tokens(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<Option<Vec<Token>>> {
		// Create the request.
		let request = super::Request::SemanticTokens(Request {
			module: module.clone(),
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::SemanticTokens(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.tokens)
	}
}

impl Compiler {
	pub(crate) async fn handle_semantic_tokens_full_request(
		&self,
		params: lsp::SemanticTokensParams,
	) -> tg::Result<Option<lsp::SemanticTokensResult>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the semantic tokens.
		let tokens = self.semantic_tokens(&module).await?;
		let Some(mut tokens) = tokens else {
			return Ok(None);
		};

		// Ensure tokens are in source order.
		tokens.sort_by_key(|token| (token.line, token.start));

		// Convert to LSP relative token encoding.
		let mut previous_line = 0;
		let mut previous_start = 0;
		let data = tokens
			.into_iter()
			.map(|token| {
				let delta_line = token.line - previous_line;
				let delta_start = if delta_line == 0 {
					token.start - previous_start
				} else {
					token.start
				};
				previous_line = token.line;
				previous_start = token.start;
				lsp::SemanticToken {
					delta_line,
					delta_start,
					length: token.length,
					token_type: token.token_type,
					token_modifiers_bitset: token.token_modifiers_bitset,
				}
			})
			.collect();

		let semantic_tokens = lsp::SemanticTokens {
			result_id: None,
			data,
		};

		Ok(Some(lsp::SemanticTokensResult::Tokens(semantic_tokens)))
	}
}
