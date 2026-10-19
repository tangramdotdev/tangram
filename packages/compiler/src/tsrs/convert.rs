use {super::Project, tangram_client::prelude::*, tsrs_lsproto as native};

pub(super) fn script_element_kind(kind: native::SymbolKind) -> tg::Result<&'static str> {
	let kind = match kind {
		native::SymbolKind::Class => "class",
		native::SymbolKind::Constant => "const",
		native::SymbolKind::Constructor => "constructor",
		native::SymbolKind::Enum => "enum",
		native::SymbolKind::EnumMember => "enum member",
		native::SymbolKind::File => "script",
		native::SymbolKind::Function => "function",
		native::SymbolKind::Interface => "interface",
		native::SymbolKind::Method => "method",
		native::SymbolKind::Module
		| native::SymbolKind::Namespace
		| native::SymbolKind::Package => "module",
		native::SymbolKind::Property | native::SymbolKind::Field => "property",
		native::SymbolKind::TypeParameter => "type parameter",
		native::SymbolKind::Variable => "var",
		_ => return Err(tg::error!("unsupported tsrs workspace symbol kind")),
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
			.ok_or_else(|| tg::error!("unknown module in a tsrs location"))?
			.without_token();
		let range = range(location.range);
		let location = tg::module::data::Location { module, range };

		Ok(location)
	}
}
