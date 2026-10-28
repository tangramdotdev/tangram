use {super::Project, crate::semantic_tokens, tangram_client::prelude::*, tsrs_lsproto as native};

impl Project {
	pub(super) fn semantic_tokens(
		&self,
		arg: &semantic_tokens::Arg,
	) -> tg::Result<semantic_tokens::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let uri = self.uri(&arg.module);
			let output = service
				.provide_semantic_tokens(context, &uri)
				.map_err(|error| tg::error!(!error, "failed to get javascript semantic tokens"))?;
			let capabilities = native::get_client_capabilities(context);
			let legend =
				tsrs_ls::semantic_tokens_legend(&capabilities.text_document.semantic_tokens);
			let tokens = output
				.semantic_tokens
				.map(|tokens| {
					let mut line = 0;
					let mut start = 0;
					let mut output = Vec::new();
					let (tokens, remainder) = tokens.data.as_chunks::<5>();
					if !remainder.is_empty() {
						return Err(tg::error!("invalid javascript semantic token data"));
					}
					for token in tokens {
						line += token[0];
						start = if token[0] == 0 {
							start + token[1]
						} else {
							token[1]
						};
						let name = legend
							.token_types
							.get(token[3] as usize)
							.ok_or_else(|| tg::error!("invalid javascript semantic token type"))?;
						let token_type = semantic_tokens::TYPES
							.iter()
							.position(|value| *value == name)
							.ok_or_else(|| {
								tg::error!("unsupported javascript semantic token type")
							})?;
						let token_type = u32::try_from(token_type).map_err(|error| {
							tg::error!(!error, "invalid javascript semantic token index")
						})?;
						let token_modifiers_bitset = legend
							.token_modifiers
							.iter()
							.enumerate()
							.fold(0, |bits, (index, name)| {
								if token[4] & (1 << index) == 0 {
									return bits;
								}
								semantic_tokens::MODIFIERS
									.iter()
									.position(|value| *value == name)
									.map_or(bits, |index| bits | (1 << index))
							});
						let token = semantic_tokens::Token {
							length: token[2],
							line,
							start,
							token_modifiers_bitset,
							token_type,
						};
						output.push(token);
					}
					Ok::<_, tg::Error>(output)
				})
				.transpose()?;
			let output = semantic_tokens::Output { tokens };

			Ok(output)
		})
	}
}
