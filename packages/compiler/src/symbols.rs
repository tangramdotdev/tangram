use {super::Compiler, lsp_types as lsp, tangram_client::prelude::*};

#[derive(Debug)]
pub struct Arg {
	pub module: tg::module::Data,
}

#[derive(Debug)]
pub struct Output {
	pub symbols: Option<Vec<Symbol>>,
}

#[derive(Debug)]
pub struct Symbol {
	pub children: Option<Vec<Self>>,
	pub detail: Option<String>,
	pub kind: Kind,
	pub name: String,
	pub range: tg::Range,
	pub selection: tg::Range,
	pub tags: Vec<Tag>,
}

#[derive(Debug)]
pub enum Kind {
	Array,
	Boolean,
	Class,
	Constant,
	Constructor,
	Enum,
	EnumMember,
	Event,
	Field,
	File,
	Function,
	Interface,
	Key,
	Method,
	Module,
	Namespace,
	Null,
	Number,
	Object,
	Operator,
	Package,
	Property,
	String,
	TypeParameter,
	Variable,
}

#[derive(Debug)]
pub enum Tag {
	Deprecated,
}

impl Compiler {
	pub async fn symbols(&self, module: &tg::module::Data) -> tg::Result<Option<Vec<Symbol>>> {
		// Create the arg.
		let arg = Arg {
			module: module.clone(),
		};

		// Get the output.
		let output = self.backend(module.kind).symbols(self, arg).await?;

		Ok(output.symbols)
	}
}

fn collect_symbol_tree(symbol: Symbol) -> lsp::DocumentSymbol {
	let Symbol {
		name,
		detail,
		kind,
		tags,
		range,
		selection: selection_range,
		children,
	} = symbol;

	let kind = match kind {
		Kind::Array => lsp::SymbolKind::ARRAY,
		Kind::Boolean => lsp::SymbolKind::BOOLEAN,
		Kind::Class => lsp::SymbolKind::CLASS,
		Kind::Constant => lsp::SymbolKind::CONSTANT,
		Kind::Constructor => lsp::SymbolKind::CONSTRUCTOR,
		Kind::Enum => lsp::SymbolKind::ENUM,
		Kind::EnumMember => lsp::SymbolKind::ENUM_MEMBER,
		Kind::Event => lsp::SymbolKind::EVENT,
		Kind::Field => lsp::SymbolKind::FIELD,
		Kind::File => lsp::SymbolKind::FILE,
		Kind::Function => lsp::SymbolKind::FUNCTION,
		Kind::Interface => lsp::SymbolKind::INTERFACE,
		Kind::Key => lsp::SymbolKind::KEY,
		Kind::Method => lsp::SymbolKind::METHOD,
		Kind::Module => lsp::SymbolKind::MODULE,
		Kind::Namespace => lsp::SymbolKind::NAMESPACE,
		Kind::Null => lsp::SymbolKind::NULL,
		Kind::Number => lsp::SymbolKind::NUMBER,
		Kind::Object => lsp::SymbolKind::OBJECT,
		Kind::Operator => lsp::SymbolKind::OPERATOR,
		Kind::Package => lsp::SymbolKind::PACKAGE,
		Kind::Property => lsp::SymbolKind::PROPERTY,
		Kind::String => lsp::SymbolKind::STRING,
		Kind::TypeParameter => lsp::SymbolKind::TYPE_PARAMETER,
		Kind::Variable => lsp::SymbolKind::VARIABLE,
	};

	let tags = tags
		.into_iter()
		.map(|tag| match tag {
			Tag::Deprecated => lsp::SymbolTag::DEPRECATED,
		})
		.collect();

	let children = children.map(|children| children.into_iter().map(collect_symbol_tree).collect());

	let range = range.into();
	let selection_range = selection_range.into();

	#[expect(deprecated)]
	lsp::DocumentSymbol {
		name,
		detail,
		kind,
		tags: Some(tags),
		range,
		selection_range,
		children,
		deprecated: None,
	}
}

impl Compiler {
	pub(super) async fn handle_document_symbol_request(
		&self,
		params: lsp::DocumentSymbolParams,
	) -> tg::Result<Option<lsp::DocumentSymbolResponse>> {
		// Get the module.
		let module = self.module_for_lsp_uri(&params.text_document.uri).await?;

		// Get the document symbols.
		let symbols = self.symbols(&module).await?;
		let Some(symbols) = symbols else {
			return Ok(None);
		};

		// Convert the symbols.
		let symbols = symbols.into_iter().map(collect_symbol_tree).collect();

		Ok(Some(lsp::DocumentSymbolResponse::Nested(symbols)))
	}
}
