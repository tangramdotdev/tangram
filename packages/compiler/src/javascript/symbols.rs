use {
	super::{Project, range},
	crate::symbols,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn symbols(&self, arg: &symbols::Arg) -> tg::Result<symbols::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let uri = self.uri(&arg.module);
			let output = service
				.provide_document_symbols(context, &uri)
				.map_err(|error| tg::error!(!error, "failed to get javascript document symbols"))?;
			let symbols = output
				.document_symbols
				.map(|symbols| symbols.into_iter().map(document_symbol).collect())
				.transpose()?;
			let output = symbols::Output { symbols };

			Ok(output)
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
			.contains(&native::SymbolTag::DEPRECATED)
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
		native::SymbolKind::ARRAY => symbols::Kind::Array,
		native::SymbolKind::BOOLEAN => symbols::Kind::Boolean,
		native::SymbolKind::CLASS => symbols::Kind::Class,
		native::SymbolKind::CONSTANT => symbols::Kind::Constant,
		native::SymbolKind::CONSTRUCTOR => symbols::Kind::Constructor,
		native::SymbolKind::ENUM => symbols::Kind::Enum,
		native::SymbolKind::ENUM_MEMBER => symbols::Kind::EnumMember,
		native::SymbolKind::EVENT => symbols::Kind::Event,
		native::SymbolKind::FIELD => symbols::Kind::Field,
		native::SymbolKind::FILE => symbols::Kind::File,
		native::SymbolKind::FUNCTION => symbols::Kind::Function,
		native::SymbolKind::INTERFACE | native::SymbolKind::STRUCT => symbols::Kind::Interface,
		native::SymbolKind::KEY => symbols::Kind::Key,
		native::SymbolKind::METHOD => symbols::Kind::Method,
		native::SymbolKind::MODULE => symbols::Kind::Module,
		native::SymbolKind::NAMESPACE => symbols::Kind::Namespace,
		native::SymbolKind::NULL => symbols::Kind::Null,
		native::SymbolKind::NUMBER => symbols::Kind::Number,
		native::SymbolKind::OBJECT => symbols::Kind::Object,
		native::SymbolKind::OPERATOR => symbols::Kind::Operator,
		native::SymbolKind::PACKAGE => symbols::Kind::Package,
		native::SymbolKind::PROPERTY => symbols::Kind::Property,
		native::SymbolKind::STRING => symbols::Kind::String,
		native::SymbolKind::TYPE_PARAMETER => symbols::Kind::TypeParameter,
		native::SymbolKind::VARIABLE => symbols::Kind::Variable,
		_ => return Err(tg::error!("unknown javascript symbol kind")),
	};

	Ok(kind)
}
