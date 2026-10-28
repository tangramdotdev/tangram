use {super::Project, tangram_client::prelude::*, tsrs_lsproto as native};

pub(super) fn script_element_kind(kind: native::SymbolKind) -> tg::Result<&'static str> {
	let kind = match kind {
		native::SymbolKind::CLASS => "class",
		native::SymbolKind::CONSTANT => "const",
		native::SymbolKind::CONSTRUCTOR => "constructor",
		native::SymbolKind::ENUM => "enum",
		native::SymbolKind::ENUM_MEMBER => "enum member",
		native::SymbolKind::FILE => "script",
		native::SymbolKind::FUNCTION => "function",
		native::SymbolKind::INTERFACE => "interface",
		native::SymbolKind::METHOD => "method",
		native::SymbolKind::MODULE
		| native::SymbolKind::NAMESPACE
		| native::SymbolKind::PACKAGE => "module",
		native::SymbolKind::PROPERTY | native::SymbolKind::FIELD => "property",
		native::SymbolKind::TYPE_PARAMETER => "type parameter",
		native::SymbolKind::VARIABLE => "var",
		_ => return Err(tg::error!("unsupported javascript workspace symbol kind")),
	};

	Ok(kind)
}

pub(super) fn position(position: tg::Position) -> native::Position {
	native::Position {
		line: position.line,
		character: position.character,
	}
}

pub(super) fn range(range: native::Range) -> tg::Range {
	let end = tg::Position {
		character: range.end.character,
		line: range.end.line,
	};
	let start = tg::Position {
		character: range.start.character,
		line: range.start.line,
	};
	tg::Range { end, start }
}

impl Project {
	pub(super) fn uri(&self, module: &tg::module::Data) -> native::DocumentUri {
		native::DocumentUri(format!("file://{}", self.host.path(module)))
	}

	pub(super) fn location(
		&self,
		location: &native::Location,
	) -> tg::Result<tg::module::data::Location> {
		let module = self
			.host
			.try_module(&location.uri.file_name())?
			.ok_or_else(|| tg::error!("unknown module in a javascript location"))?
			.without_token();
		let range = range(location.range);
		let location = tg::module::data::Location { module, range };

		Ok(location)
	}
}
