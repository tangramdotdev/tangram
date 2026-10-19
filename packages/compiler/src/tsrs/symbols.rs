use {
	super::{Project, range},
	crate::symbols,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn symbols(&self, request: &symbols::Request) -> tg::Result<symbols::Response> {
		self.with_language_service(&request.module, |context, service| {
			let uri = self.uri(&request.module);
			let output = service
				.provide_document_symbols(context, &uri)
				.map_err(|error| tg::error!(!error, "failed to get tsrs document symbols"))?;
			let symbols = output
				.document_symbols
				.map(|symbols| symbols.into_iter().map(document_symbol).collect())
				.transpose()?;
			let response = symbols::Response { symbols };

			Ok(response)
		})
	}
}

fn document_symbol(symbol: native::DocumentSymbol) -> tg::Result<symbols::Symbol> {
	let children = symbol
		.children
		.map(|children| children.into_iter().map(document_symbol).collect())
		.transpose()?;
	let detail = symbol.detail;
	let kind = symbol_kind(symbol.kind)?;
	let name = symbol.name;
	let selection = range(symbol.selection_range);
	let range = range(symbol.range);
	let tags = if symbol.deprecated.unwrap_or(false)
		|| symbol
			.tags
			.unwrap_or_default()
			.contains(&native::SymbolTag::Deprecated)
	{
		vec![symbols::Tag::Deprecated]
	} else {
		Vec::new()
	};
	let symbol = symbols::Symbol {
		children,
		detail,
		kind,
		name,
		range,
		selection,
		tags,
	};

	Ok(symbol)
}

fn symbol_kind(kind: native::SymbolKind) -> tg::Result<symbols::Kind> {
	let kind = match kind {
		native::SymbolKind::Array => symbols::Kind::Array,
		native::SymbolKind::Boolean => symbols::Kind::Boolean,
		native::SymbolKind::Class => symbols::Kind::Class,
		native::SymbolKind::Constant => symbols::Kind::Constant,
		native::SymbolKind::Constructor => symbols::Kind::Constructor,
		native::SymbolKind::Enum => symbols::Kind::Enum,
		native::SymbolKind::EnumMember => symbols::Kind::EnumMember,
		native::SymbolKind::Event => symbols::Kind::Event,
		native::SymbolKind::Field => symbols::Kind::Field,
		native::SymbolKind::File => symbols::Kind::File,
		native::SymbolKind::Function => symbols::Kind::Function,
		native::SymbolKind::Interface | native::SymbolKind::Struct => symbols::Kind::Interface,
		native::SymbolKind::Key => symbols::Kind::Key,
		native::SymbolKind::Method => symbols::Kind::Method,
		native::SymbolKind::Module => symbols::Kind::Module,
		native::SymbolKind::Namespace => symbols::Kind::Namespace,
		native::SymbolKind::Null => symbols::Kind::Null,
		native::SymbolKind::Number => symbols::Kind::Number,
		native::SymbolKind::Object => symbols::Kind::Object,
		native::SymbolKind::Operator => symbols::Kind::Operator,
		native::SymbolKind::Package => symbols::Kind::Package,
		native::SymbolKind::Property => symbols::Kind::Property,
		native::SymbolKind::String => symbols::Kind::String,
		native::SymbolKind::TypeParameter => symbols::Kind::TypeParameter,
		native::SymbolKind::Variable => symbols::Kind::Variable,
		_ => return Err(tg::error!("unknown tsrs symbol kind")),
	};

	Ok(kind)
}
