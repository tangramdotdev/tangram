use {
	super::Project, crate::document_link, std::collections::BTreeSet, tangram_client::prelude::*,
	tsrs_ast::Kind,
};

impl Project {
	pub(super) fn document_link(
		&self,
		request: &document_link::Request,
	) -> tg::Result<document_link::Response> {
		let path = self.host.path(&request.module);
		let file = self
			.program()?
			.get_source_file(&path)
			.ok_or_else(|| tg::error!("missing source in the tsrs program"))?;
		let encoding = *self.host.compiler.position_encoding.read().unwrap();
		let mut links = Vec::new();
		let mut seen = BTreeSet::new();
		for &specifier in file.imports() {
			let Some(parent) = specifier.parent() else {
				continue;
			};
			let is_import = match parent.kind() {
				Kind::CallExpression => parent
					.expression()
					.is_some_and(|expression| expression.kind() == Kind::ImportKeyword),
				Kind::ExportDeclaration | Kind::ImportDeclaration | Kind::JSImportDeclaration => {
					true
				},
				_ => false,
			};
			if !is_import {
				continue;
			}
			let attributes = tsrs_module::get_module_resolution_attributes(specifier).map(|_| {
				tsrs_module::module_resolution_attributes(specifier)
					.into_iter()
					.map(|(key, value)| (key.to_owned(), value.to_owned()))
					.collect()
			});
			let Some(target) = self.host.resolve(specifier.text(), &path, attributes)? else {
				continue;
			};
			let module = self
				.host
				.try_module(&target)?
				.ok_or_else(|| tg::error!("unknown module in a tsrs import link"))?
				.without_token();
			let start = usize::try_from(tsrs_ls::astnav::get_start_of_node(specifier, file, false))
				.map_err(|error| tg::error!(!error, "invalid tsrs import position"))?
				.checked_add(1)
				.ok_or_else(|| tg::error!("invalid tsrs import position"))?;
			let end = specifier
				.end()
				.source_offset()
				.ok_or_else(|| tg::error!("missing tsrs import position"))?;
			let end = usize::try_from(end)
				.map_err(|error| tg::error!(!error, "invalid tsrs import position"))?;
			let end = end
				.checked_sub(1)
				.ok_or_else(|| tg::error!("invalid tsrs import position"))?;
			let range = tg::Range::try_from_byte_range_in_string(file.text(), start..end, encoding)
				.ok_or_else(|| tg::error!("invalid tsrs import link range"))?;
			let key = (
				range.start.line,
				range.start.character,
				range.end.line,
				range.end.character,
				module.to_string(),
			);
			if seen.insert(key) {
				let link = document_link::Link {
					module,
					range,
					tooltip: None,
				};
				links.push(link);
			}
		}
		let links = (!links.is_empty()).then_some(links);
		let response = document_link::Response { links };

		Ok(response)
	}
}
