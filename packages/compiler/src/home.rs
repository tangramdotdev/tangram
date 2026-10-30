use {
	super::{Compiler, bindings_language, config, load},
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

impl Compiler {
	/// Convert locations that a language service returned for a module of the given kind to LSP locations, mapping the locations in bindings to the homes of their modules' interfaces, dropping the locations that have no place there, and removing duplicates, so that bindings never appear in results.
	pub(crate) async fn lsp_locations(
		&self,
		locations: impl IntoIterator<Item = tg::module::data::Location>,
		kind: tg::module::Kind,
	) -> tg::Result<Vec<lsp::Location>> {
		let locations = locations
			.into_iter()
			.map(|location| {
				let compiler = self.clone();
				async move { compiler.try_lsp_location(&location, kind).await }
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let mut output = Vec::new();
		for location in locations.into_iter().flatten() {
			if !output.contains(&location) {
				output.push(location);
			}
		}
		Ok(output)
	}

	/// Convert ranges in a module that a language service returned for a module of the given kind to LSP ranges in the home of the module's interface, dropping the ranges that have no place there.
	pub(crate) async fn lsp_ranges(
		&self,
		module: &tg::module::Data,
		ranges: Vec<tg::Range>,
		kind: tg::module::Kind,
	) -> tg::Result<Vec<lsp::Range>> {
		let mut output = Vec::new();
		for range in ranges {
			let location = tg::module::data::Location {
				module: module.clone(),
				range,
			};
			if let Some(location) = self.try_lsp_location(&location, kind).await? {
				output.push(location.range);
			}
		}
		Ok(output)
	}

	/// Convert a location that a language service returned for a module of the given kind to an LSP location, mapping a location in bindings to the home of its module's interface, or returning `None` if it has no place there.
	pub(crate) async fn try_lsp_location(
		&self,
		location: &tg::module::data::Location,
		kind: tg::module::Kind,
	) -> tg::Result<Option<lsp::Location>> {
		// Use a location outside of bindings as it is.
		let Some(language) = bindings_language(kind, location.module.kind) else {
			let uri = self.lsp_uri_for_module(&location.module).await?;
			let range = location.range.into();
			let location = lsp::Location { uri, range };
			return Ok(Some(location));
		};

		// Map the position of an export in the bindings to its name in the home.
		let home = self.interface_home(&location.module).await?;
		let text = self
			.load_module_with_language(&location.module, Some(language))
			.await?;
		let encoding = *self.position_encoding.read().unwrap();
		if let Some(offset) = location
			.range
			.start
			.try_to_byte_index_in_string(&text, encoding)
			&& let Some(source) = load::source(&text, offset)
		{
			let uri = self.lsp_uri_for_module(&home).await?;
			let range = source.range(encoding).into();
			let location = lsp::Location { uri, range };
			return Ok(Some(location));
		}

		// Map a location at the start of the bindings, which is where the language service locates the module itself, to the start of the home.
		let start = tg::Position {
			character: 0,
			line: 0,
		};
		if location.range.start == start {
			let uri = self.lsp_uri_for_module(&home).await?;
			let range = tg::Range { end: start, start }.into();
			let location = lsp::Location { uri, range };
			return Ok(Some(location));
		}

		Ok(None)
	}

	/// Determine whether a language service for a module of the given kind may edit a module, which it may not if it sees the module through bindings, because positions in bindings do not apply to the module that they were generated from.
	pub(crate) fn is_editable(kind: tg::module::Kind, module: &tg::module::Data) -> bool {
		bindings_language(kind, module.kind).is_none()
	}

	/// Get the home of a module's interface, which is the module whose text the interface's ranges describe: a config's interface file, if it has one, and otherwise the module itself.
	pub(crate) async fn interface_home(
		&self,
		module: &tg::module::Data,
	) -> tg::Result<tg::module::Data> {
		if !module.kind.is_config() {
			return Ok(module.clone());
		}
		let text = self.load_module_inner(module, None).await?;
		let home = config::try_get_interface_module(&self.instance, module, &text)
			.await?
			.unwrap_or_else(|| module.clone());
		Ok(home)
	}
}
