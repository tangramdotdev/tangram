use {
	futures::{StreamExt as _, future::try_join_all, stream::FuturesUnordered},
	std::{
		collections::{HashMap, HashSet},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

/// The preloaded modules and the resolutions of their static imports, which allow an engine's synchronous module callbacks to find a module without waiting.
#[derive(Default)]
pub(crate) struct Modules {
	entries: HashMap<String, Arc<Entry>>,
	resolutions: HashMap<(Option<String>, tg::module::Import), tg::module::Data>,
}

/// A preloaded module.
pub(crate) struct Entry {
	pub(crate) module: tg::module::Data,
	pub(crate) source_map: String,
	pub(crate) text: String,
	pub(crate) transpiled: bool,
}

pub(crate) struct Arg {
	pub(crate) graphs: Arc<tangram_compiler::graphs::Graphs>,
	pub(crate) import: tg::module::Import,
	pub(crate) instance: tg::instance::dynamic::Instance,
	pub(crate) known: HashSet<String>,
	pub(crate) referrer: Option<tg::module::Data>,
}

pub(crate) struct Output {
	pub(crate) module: tg::module::Data,
	pub(crate) modules: Modules,
}

struct Loaded {
	entry: Entry,
	imports: Vec<(tg::module::Import, tg::module::Data)>,
}

/// Resolve an import and load the modules that it reaches through static imports, concurrently, skipping the modules that are known.
pub(crate) async fn preload(arg: Arg) -> tg::Result<Output> {
	let Arg {
		graphs,
		import,
		instance,
		known,
		referrer,
	} = arg;

	// Resolve the import.
	let module = resolve(&graphs, &instance, referrer.as_ref(), &import).await?;
	let mut modules = Modules::default();
	modules
		.resolutions
		.insert((referrer.as_ref().map(key), import), module.clone());

	// Load the modules, following the static imports of each.
	let mut seen = known;
	let mut tasks = FuturesUnordered::new();
	if seen.insert(key(&module)) {
		tasks.push(load(&graphs, &instance, module.clone()));
	}
	while let Some(result) = tasks.next().await {
		let Loaded { entry, imports } = result?;
		let referrer = key(&entry.module);
		for (import, module) in imports {
			if seen.insert(key(&module)) {
				tasks.push(load(&graphs, &instance, module.clone()));
			}
			modules
				.resolutions
				.insert((Some(referrer.clone()), import), module);
		}
		modules.entries.insert(referrer, Arc::new(entry));
	}

	let output = Output { module, modules };

	Ok(output)
}

impl Modules {
	/// Get the preloaded module.
	pub(crate) fn entry(&self, module: &tg::module::Data) -> Option<Arc<Entry>> {
		self.entries.get(&key(module)).cloned()
	}

	/// Add other preloaded modules.
	pub(crate) fn extend(&mut self, other: Self) {
		self.entries.extend(other.entries);
		self.resolutions.extend(other.resolutions);
	}

	/// Get the keys of the preloaded modules.
	pub(crate) fn known(&self) -> HashSet<String> {
		self.entries.keys().cloned().collect()
	}

	/// Get the module that an import resolved to.
	pub(crate) fn resolution(
		&self,
		referrer: Option<&tg::module::Data>,
		import: &tg::module::Import,
	) -> Option<tg::module::Data> {
		self.resolutions
			.get(&(referrer.map(key), import.clone()))
			.cloned()
	}
}

/// Load and transpile a module, and resolve the static imports that remain after transpiling.
async fn load(
	graphs: &tangram_compiler::graphs::Graphs,
	instance: &tg::instance::dynamic::Instance,
	mut module: tg::module::Data,
) -> tg::Result<Loaded> {
	// Load the module without the instance if possible.
	let language = tg::module::load::Language::JavaScript;
	let output = if let Some(output) = graphs.try_load_module(instance, &module, language).await {
		output
	} else {
		let arg = tg::module::load::Arg {
			language: Some(language),
			module: module.clone(),
		};
		instance.load_module(arg).await.map_err(|error| {
			tg::error!(
				!error,
				module = ?module.without_token(),
				"failed to load the module"
			)
		})?
	};
	module.referent.options.tokens.inherit(&output.tokens);

	// Transpile the module.
	let transpiled = tangram_compiler::Compiler::transpile(&output.text, &module);
	if !transpiled.diagnostics.is_empty() {
		let diagnostics = transpiled
			.diagnostics
			.into_iter()
			.filter_map(|diagnostic| diagnostic.try_into().ok())
			.collect();
		return Err(tg::error!(
			diagnostics = diagnostics,
			"failed to transpile the module"
		));
	}

	// Resolve the static imports.
	let analysis = tangram_compiler::Compiler::analyze_javascript(&module, &transpiled.text);
	let imports = try_join_all(analysis.static_imports.into_iter().map(|import| {
		let module = &module;
		async move {
			let resolved = resolve(graphs, instance, Some(module), &import).await?;
			Ok::<_, tg::Error>((import, resolved))
		}
	}))
	.await?;

	let entry = Entry {
		transpiled: transpiled.text != output.text,
		module,
		source_map: transpiled.source_map,
		text: transpiled.text,
	};
	let loaded = Loaded { entry, imports };

	Ok(loaded)
}

/// Resolve an import.
async fn resolve(
	graphs: &tangram_compiler::graphs::Graphs,
	instance: &tg::instance::dynamic::Instance,
	referrer: Option<&tg::module::Data>,
	import: &tg::module::Import,
) -> tg::Result<tg::module::Data> {
	// Resolve the module without the instance if possible.
	if let Some(referrer) = referrer
		&& let Some(module) = graphs.try_resolve_module(instance, referrer, import).await
	{
		return Ok(module);
	}

	let arg = tg::module::resolve::Arg {
		referrer: referrer.cloned(),
		import: import.clone(),
	};
	let output = instance.resolve_module(arg).await.map_err(|error| {
		tg::error!(
			!error,
			import = ?import.without_token(),
			referrer = ?referrer.map(tg::module::Data::without_token),
			"failed to resolve the module"
		)
	})?;

	Ok(output.module)
}

/// Get the key of a module, which does not depend on its tokens.
fn key(module: &tg::module::Data) -> String {
	module.without_token().to_string()
}
