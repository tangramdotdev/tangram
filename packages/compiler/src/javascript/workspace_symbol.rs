use {
	super::{Project, convert::script_element_kind},
	crate::workspace_symbols,
	tangram_client::prelude::*,
	tsrs_ls::Host as _,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn workspace_symbols(
		&self,
		arg: &workspace_symbols::Arg,
	) -> tg::Result<workspace_symbols::Output> {
		let context = Self::context();
		let program = self.program()?;
		let converters = self.snapshot.borrow().0.converters();
		let preferences = self.snapshot.borrow().0.get_preferences("");
		let output = tsrs_ls::provide_workspace_symbols(
			&context,
			&[program],
			&converters,
			&preferences,
			&arg.query,
		)
		.map_err(|error| tg::error!(!error, "failed to get javascript workspace symbols"))?;
		let symbols = output
			.symbol_informations
			.map(|symbols| {
				symbols
					.into_iter()
					.map(|symbol| {
						let location = self.location(&symbol.location)?;
						let kind = script_element_kind(symbol.kind)?.to_owned();
						let deprecated = symbol.deprecated.or_else(|| {
							symbol
								.tags
								.as_ref()
								.map(|tags| tags.contains(&native::SymbolTag::DEPRECATED))
						});
						let symbol = workspace_symbols::Symbol {
							container_name: symbol.container_name,
							deprecated,
							kind,
							module: location.module,
							name: symbol.name,
							range: location.range,
						};
						Ok::<_, tg::Error>(symbol)
					})
					.collect()
			})
			.transpose()?;
		let output = workspace_symbols::Output { symbols };

		Ok(output)
	}
}
