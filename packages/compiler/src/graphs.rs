use {
	std::{collections::HashMap, sync::Mutex},
	tangram_client::prelude::*,
};

/// The graphs that a runtime has loaded, so that resolving and loading the modules of a graph loads the graph once.
#[derive(Default)]
pub struct Graphs(Mutex<HashMap<tg::graph::Id, tg::Graph, tg::id::BuildHasher>>);

impl Graphs {
	/// Resolve an import of a file in a graph to another file in the same graph without the instance, returning `None` when the instance must resolve it.
	pub async fn try_resolve_module<I>(
		&self,
		instance: &I,
		referrer: &tg::module::Data,
		import: &tg::module::Import,
	) -> Option<tg::module::Data>
	where
		I: tg::Instance,
	{
		self.try_resolve_module_inner(instance, referrer, import)
			.await
			.ok()
			.flatten()
	}

	async fn try_resolve_module_inner<I>(
		&self,
		instance: &I,
		referrer: &tg::module::Data,
		import: &tg::module::Import,
	) -> tg::Result<Option<tg::module::Data>>
	where
		I: tg::Instance,
	{
		// Only resolve imports of modules or files.
		if !matches!(
			import.kind,
			None | Some(
				tg::module::Kind::JavaScript
					| tg::module::Kind::Python
					| tg::module::Kind::TypeScript
					| tg::module::Kind::TypeScriptDeclaration
					| tg::module::Kind::File
			)
		) {
			return Ok(None);
		}

		// Get the referrer's file.
		let Some((graph, file)) = self.try_get_file(&referrer.referent) else {
			return Ok(None);
		};

		// Get the dependency, which must be a file in the same graph.
		let dependency = file
			.get_dependency_edge_with_instance(instance, &import.reference)
			.await?;
		let Some(edge) = dependency.0.node else {
			return Ok(None);
		};
		let tg::graph::Edge::Pointer(pointer) = &edge else {
			return Ok(None);
		};
		let pointer = pointer.clone();
		if pointer.kind != tg::artifact::Kind::File || pointer.graph.id() != graph.id() {
			return Ok(None);
		}

		// Create the referent, which inherits the referrer's tokens, including its token for the graph.
		let mut referent = tg::Referent {
			node: tg::module::data::Source::Edge(edge.to_data()),
			options: dependency.0.options,
		};
		referent.inherit(&referrer.referent);

		// Get the kind from the import, the path's extension, or the file's module field.
		let kind = if let Some(kind) = import.kind {
			kind
		} else if let Some(kind) = referent
			.path()
			.and_then(|path| tg::module::module_kind_for_path(path).ok())
		{
			kind
		} else {
			let file = tg::Artifact::with_pointer(pointer)
				.try_unwrap_file()
				.ok()
				.ok_or_else(|| tg::error!("expected a file"))?;
			file.module_with_instance(instance)
				.await?
				.unwrap_or(tg::module::Kind::File)
		};

		// Create the module.
		let module = tg::module::Data { kind, referent };

		Ok(Some(module))
	}

	/// Load a module that is a file in a graph and is written in the language without the instance, returning `None` when the instance must load it.
	pub async fn try_load_module<I>(
		&self,
		instance: &I,
		module: &tg::module::Data,
		language: tg::module::load::Language,
	) -> Option<tg::module::load::Output>
	where
		I: tg::Instance,
	{
		self.try_load_module_inner(instance, module, language)
			.await
			.ok()
			.flatten()
	}

	async fn try_load_module_inner<I>(
		&self,
		instance: &I,
		module: &tg::module::Data,
		language: tg::module::load::Language,
	) -> tg::Result<Option<tg::module::load::Output>>
	where
		I: tg::Instance,
	{
		// Only load modules written in the language.
		if !matches!(
			(language, module.kind),
			(
				tg::module::load::Language::JavaScript,
				tg::module::Kind::JavaScript | tg::module::Kind::TypeScript
			) | (tg::module::load::Language::Python, tg::module::Kind::Python)
		) {
			return Ok(None);
		}

		// Get the module's file.
		let Some((_, file)) = self.try_get_file(&module.referent) else {
			return Ok(None);
		};

		// Read the file's contents.
		let contents = file.contents_with_instance(instance).await?;
		let text = contents.text_with_instance(instance).await?;

		// Prepare the text as the server does.
		let text = crate::load::module(module, &text, Some(language))?;

		// Create the output.
		let output = tg::module::load::Output {
			text,
			tokens: tg::authorization::Tokens::default(),
		};

		Ok(Some(output))
	}

	/// Get the file that a local referent points to in a graph, using the loaded graph if there is one.
	pub fn try_get_file(
		&self,
		referent: &tg::Referent<tg::module::data::Source>,
	) -> Option<(tg::Graph, tg::File)> {
		// Get the pointer.
		let tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer)) =
			&referent.node
		else {
			return None;
		};
		if pointer.kind != tg::artifact::Kind::File
			|| !referent
				.options
				.location
				.as_ref()
				.is_none_or(tg::Location::is_local)
		{
			return None;
		}

		// Get or insert the graph.
		let graph = self
			.0
			.lock()
			.unwrap()
			.entry(pointer.graph.clone())
			.or_insert_with(|| tg::Graph::with_id(pointer.graph.clone()))
			.clone();

		// Add the referent's tokens to a graph that has not been loaded, so that a failed load can be retried with newer tokens.
		if graph.state().object().is_none() {
			graph.state().inherit_tokens(&referent.options.tokens);
		}

		// Create the file.
		let pointer = tg::graph::Pointer {
			graph: graph.clone(),
			index: pointer.index,
			kind: pointer.kind,
		};
		let file = tg::Artifact::with_pointer(pointer).try_unwrap_file().ok()?;
		file.state().set_tokens(referent.options.tokens.clone());

		Some((graph, file))
	}
}
