use {
	super::Database, ruff_db::files::File, tangram_client::prelude::*, ty_python_semantic::Db as _,
};

impl Database {
	pub(super) fn symbols(
		&self,
		file: File,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::symbols::Response> {
		let tree = ty_ide::document_symbols(self, self.program_file(file)).to_hierarchical();
		let symbols = tree
			.iter()
			.map(|(id, info)| self.symbol(file, &tree, id, info, encoding))
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::symbols::Response {
			symbols: Some(symbols),
		})
	}

	fn symbol(
		&self,
		file: File,
		tree: &ty_ide::HierarchicalSymbols,
		id: ty_ide::SymbolId,
		info: ty_ide::SymbolInfo<'_>,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::symbols::Symbol> {
		use {crate::symbols::Kind, ty_ide::SymbolKind as TyKind};
		let kind = match info.kind {
			TyKind::Class => Kind::Class,
			TyKind::Constant => Kind::Constant,
			TyKind::Constructor => Kind::Constructor,
			TyKind::Field => Kind::Field,
			TyKind::Function => Kind::Function,
			TyKind::Import | TyKind::Module => Kind::Module,
			TyKind::Method => Kind::Method,
			TyKind::Parameter | TyKind::Variable => Kind::Variable,
			TyKind::Property => Kind::Property,
			TyKind::TypeParameter => Kind::TypeParameter,
		};
		let children = tree
			.children(id)
			.map(|(id, info)| self.symbol(file, tree, id, info, encoding))
			.collect::<tg::Result<Vec<_>>>()?;
		let range = self.range(file, info.full_range, encoding)?;
		let selection = self.range(file, info.name_range, encoding)?;
		let tags = if info.deprecated {
			vec![crate::symbols::Tag::Deprecated]
		} else {
			Vec::new()
		};
		let symbol = crate::symbols::Symbol {
			children: Some(children),
			detail: None,
			kind,
			name: info.name.into_owned(),
			range,
			selection,
			tags,
		};
		Ok(symbol)
	}

	pub(super) fn workspace_symbols(
		&self,
		query: &str,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::workspace_symbols::Response> {
		let symbols = ty_ide::workspace_symbols(self, query)
			.into_iter()
			.map(|symbol| {
				let location = self.location(symbol.file, symbol.symbol.name_range, encoding)?;
				Ok(crate::workspace_symbols::Symbol {
					container_name: None,
					deprecated: Some(symbol.symbol.deprecated),
					kind: kind(symbol.symbol.kind).to_owned(),
					module: location.module,
					name: symbol.symbol.name.into_owned(),
					range: location.range,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(crate::workspace_symbols::Response {
			symbols: Some(symbols),
		})
	}

	pub(super) fn call_item(
		&self,
		item: &ty_ide::CallHierarchyItem,
		encoding: tg::position::Encoding,
	) -> tg::Result<crate::call_hierarchy::Item> {
		let location = self.location(item.file, item.selection_range, encoding)?;
		let range = if self
			.entry(item.file)
			.is_some_and(|entry| entry.module.kind != tg::module::Kind::Py)
		{
			location.range
		} else {
			self.range(item.file, item.full_range, encoding)?
		};
		let data = crate::call_hierarchy::ItemData {
			module: location.module.clone(),
			position: location.range.start,
		};
		let item = crate::call_hierarchy::Item {
			container_name: None,
			data: Some(data),
			detail: None,
			kind: kind(item.kind).to_owned(),
			module: location.module,
			name: item.name.to_string(),
			range,
			selection: location.range,
		};
		Ok(item)
	}
}

fn kind(kind: ty_ide::SymbolKind) -> &'static str {
	match kind {
		ty_ide::SymbolKind::Class => "class",
		ty_ide::SymbolKind::Constant => "const",
		ty_ide::SymbolKind::Constructor => "constructor",
		ty_ide::SymbolKind::Field | ty_ide::SymbolKind::Property => "property",
		ty_ide::SymbolKind::Function => "function",
		ty_ide::SymbolKind::Import | ty_ide::SymbolKind::Module => "module",
		ty_ide::SymbolKind::Method => "method",
		ty_ide::SymbolKind::Parameter | ty_ide::SymbolKind::Variable => "var",
		ty_ide::SymbolKind::TypeParameter => "type parameter",
	}
}
