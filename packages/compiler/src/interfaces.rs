use {
	crate::{Compiler, document::Key},
	futures::future,
	petgraph::graph::DiGraph,
	std::{
		collections::{BTreeMap, BTreeSet},
		hash::{Hash as _, Hasher as _},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

/// The arg of getting the interface of a module.
pub(crate) struct Arg {
	/// The module.
	pub(crate) module: tg::module::Data,

	/// The modules in other languages that getting the interface loads without their interfaces, which are the other members of the module's cycle.
	pub(crate) untyped: BTreeSet<Key>,
}

/// The graph of the modules that the checkers reach, with the interface of each module that a checker loads from another language.
#[derive(Default)]
pub(crate) struct Interfaces {
	/// The modules in other languages that the JavaScript checker loads.
	javascript: Mutex<Loaded>,

	/// The lock that serializes preparing interfaces.
	lock: tokio::sync::Mutex<()>,

	/// The modules, keyed by their identities.
	nodes: Mutex<BTreeMap<Key, Node>>,

	/// The modules in other languages that the Python checker loads.
	python: Mutex<Loaded>,
}

/// The modules in other languages that a checker loads.
#[derive(Default)]
struct Loaded {
	/// The modules.
	modules: BTreeMap<Key, tg::module::Data>,

	/// The modules whose interfaces have been prepared, which the checker loads with their interfaces.
	prepared: BTreeSet<Key>,

	/// Whether the checker has loaded a module whose interface has not been prepared.
	unprepared: bool,
}

#[derive(Clone)]
struct Node {
	imports: Vec<tg::module::Data>,
	interface: Option<Arc<Entry>>,
	module: tg::module::Data,
	version: u64,
}

/// An interface that has been gotten for a module.
pub(crate) struct Entry {
	/// The hash of the versions of the modules that the module reaches when its interface was gotten.
	fingerprint: u64,

	/// The interface.
	pub(crate) interface: tg::Interface,

	/// The hash of the interface.
	pub(crate) version: u64,
}

impl Compiler {
	/// Get the interface of a module, with the types that its checker infers.
	pub async fn interface(&self, module: &tg::module::Data) -> tg::Result<tg::Interface> {
		let mut errors = self
			.prepare_interfaces_inner(vec![module.clone()], true)
			.await;
		if let Some(error) = errors.remove(&key(module)) {
			return Err(error);
		}
		let entry = self
			.try_get_interface(module)
			.ok_or_else(|| tg::error!("failed to get the interface of the module"))?;

		// Remove the tokens of the imports, which belong to this compiler's session.
		let mut interface = entry.interface.clone();
		for module in interface.imports.values_mut() {
			*module = module.without_token();
		}

		Ok(interface)
	}

	/// Get the interfaces of the modules in other languages that a checker has loaded, in dependency order, so that getting no interface waits on another checker.
	pub(crate) async fn prepare_interfaces(&self, language: tg::module::load::Language) {
		loop {
			// Prepare the interfaces of the modules that the checker has loaded.
			let modules = self
				.interfaces
				.loaded(language)
				.lock()
				.unwrap()
				.modules
				.clone();
			if modules.is_empty() {
				return;
			}
			let roots = modules.values().cloned().collect();
			self.prepare_interfaces_inner(roots, true).await;
			let mut loaded = self.interfaces.loaded(language).lock().unwrap();
			loaded.prepared.extend(modules.keys().cloned());

			// Add the modules in other languages that the interfaces import, whose bindings the checker loads with the bindings that import them, and stop when there are none.
			let imports = modules
				.keys()
				.filter_map(|key| {
					self.interfaces
						.nodes
						.lock()
						.unwrap()
						.get(key)?
						.interface
						.clone()
				})
				.flat_map(|entry| {
					entry
						.interface
						.imports
						.values()
						.cloned()
						.collect::<Vec<_>>()
				})
				.filter(|module| {
					crate::load::bindings_language(module, Some(language))
						.ok()
						.flatten()
						.is_some()
				})
				.filter(|module| !loaded.modules.contains_key(&key(module)))
				.collect::<Vec<_>>();
			if imports.is_empty() {
				return;
			}
			for module in imports {
				loaded.modules.insert(key(&module), module);
			}
		}
	}

	/// Record that a checker loads a module, and determine whether to load it with its interface, which is only if the module is in another language and its interface has been prepared.
	pub(crate) fn load_typed(
		&self,
		language: tg::module::load::Language,
		module: &tg::module::Data,
	) -> bool {
		if crate::load::bindings_language(module, Some(language))
			.ok()
			.flatten()
			.is_none()
		{
			return true;
		}
		let key = key(module);
		let mut loaded = self.interfaces.loaded(language).lock().unwrap();
		if loaded.prepared.contains(&key) {
			return true;
		}
		loaded.modules.insert(key, module.clone());
		loaded.unprepared = true;
		false
	}

	/// Determine whether a checker has loaded a module whose interface has not been prepared since the last call.
	pub(crate) fn take_unprepared(&self, language: tg::module::load::Language) -> bool {
		std::mem::take(&mut self.interfaces.loaded(language).lock().unwrap().unprepared)
	}

	/// Get the modules in the interface graph.
	pub(crate) fn interface_keys(&self) -> Vec<Key> {
		self.interfaces
			.nodes
			.lock()
			.unwrap()
			.keys()
			.cloned()
			.collect()
	}

	/// Forget the modules in other languages that a checker no longer loads.
	pub(crate) fn retain_loaded(&self, language: tg::module::load::Language, keys: &BTreeSet<Key>) {
		let mut loaded = self.interfaces.loaded(language).lock().unwrap();
		loaded.modules.retain(|key, _| keys.contains(key));
		loaded.prepared.retain(|key| keys.contains(key));
	}

	/// Get the interfaces of the modules in other languages that the roots load, and of the roots too if `include_roots` is true, returning the errors of the modules whose interfaces could not be gotten.
	async fn prepare_interfaces_inner(
		&self,
		roots: Vec<tg::module::Data>,
		include_roots: bool,
	) -> BTreeMap<Key, tg::Error> {
		let _guard = self.interfaces.lock.lock().await;

		// Discover the modules that the roots reach, getting the modules of each level concurrently.
		let mut nodes = BTreeMap::new();
		let mut pending = roots.clone();
		while !pending.is_empty() {
			let mut keys = BTreeSet::new();
			let modules = pending
				.drain(..)
				.filter(|module| {
					let key = key(module);
					language(module.kind).is_some() && !nodes.contains_key(&key) && keys.insert(key)
				})
				.collect::<Vec<_>>();
			let output =
				future::join_all(modules.into_iter().map(|module| self.node(module))).await;
			for node in output {
				pending.extend(node.imports.iter().cloned());
				nodes.insert(key(&node.module), node);
			}
		}

		// Find the interfaces that each module's checker loads from other languages.
		let dependencies = cross_language_imports(&nodes);
		let mut targets = roots
			.iter()
			.filter_map(|module| dependencies.get(&key(module)))
			.flatten()
			.cloned()
			.collect::<BTreeSet<_>>();
		if include_roots {
			targets.extend(
				roots
					.iter()
					.map(key)
					.filter(|key| dependencies.contains_key(key)),
			);
		}

		// Get the interfaces of each group of mutually dependent modules after the groups they depend on, and remove the interface of a module whose interface cannot be gotten.
		let mut errors = BTreeMap::new();
		for component in components(&dependencies, &targets) {
			let fingerprints = component
				.iter()
				.map(|key| (key.clone(), fingerprint(&nodes, key)))
				.collect::<BTreeMap<_, _>>();
			let current = component.iter().all(|key| {
				nodes[key]
					.interface
					.as_ref()
					.is_some_and(|entry| entry.fingerprint == fingerprints[key])
			});
			if current {
				continue;
			}
			let mut entries = Vec::new();
			for key in &component {
				let module = nodes[key].module.clone();
				let untyped = component.iter().cloned().collect();
				let arg = Arg { module, untyped };
				let interface = match self.backend(arg.module.kind).interface(self, arg).await {
					Ok(interface) => interface,
					Err(error) => {
						tracing::debug!(error = %error.trace(), "failed to get the interface");
						errors.insert(key.clone(), error);
						entries.push((key.clone(), None));
						continue;
					},
				};
				let entry = Entry {
					fingerprint: fingerprints[key],
					version: hash(&interface),
					interface,
				};
				entries.push((key.clone(), Some(Arc::new(entry))));
			}
			let mut shared = self.interfaces.nodes.lock().unwrap();
			for (key, entry) in entries {
				let node = nodes.get_mut(&key).unwrap();
				node.interface = entry;
				shared.insert(key, node.clone());
			}
		}

		// Store the modules, and remove the modules that neither the roots, the open documents, nor the modules that the checkers load reach.
		let documents = self.list_documents().await;
		let loaded = [&self.interfaces.javascript, &self.interfaces.python]
			.into_iter()
			.flat_map(|loaded| {
				loaded
					.lock()
					.unwrap()
					.modules
					.keys()
					.cloned()
					.collect::<Vec<_>>()
			})
			.collect::<Vec<_>>();
		let mut shared = self.interfaces.nodes.lock().unwrap();
		shared.extend(nodes);
		let mut reachable = BTreeSet::new();
		let mut pending = roots
			.iter()
			.chain(&documents)
			.map(key)
			.chain(loaded)
			.collect::<Vec<_>>();
		while let Some(key) = pending.pop() {
			if !reachable.insert(key.clone()) {
				continue;
			}
			if let Some(node) = shared.get(&key) {
				pending.extend(node.imports.iter().map(self::key));
			}
		}
		shared.retain(|key, _| reachable.contains(key));
		drop(shared);

		errors
	}

	/// Get the node of a module, reusing the cached node if the module is unchanged.
	async fn node(&self, module: tg::module::Data) -> Node {
		let version = self.get_module_version(&module).await.unwrap_or_default();
		let cached = self
			.interfaces
			.nodes
			.lock()
			.unwrap()
			.get(&key(&module))
			.cloned();
		match cached {
			Some(node) if node.version == version => node,
			cached => {
				let imports = self.module_imports(&module).await.unwrap_or_default();
				let interface = cached.and_then(|node| node.interface);
				Node {
					imports,
					interface,
					module,
					version,
				}
			},
		}
	}

	/// Get the cached interface of a module, if there is one.
	pub(crate) fn try_get_interface(&self, module: &tg::module::Data) -> Option<Arc<Entry>> {
		self.interfaces
			.nodes
			.lock()
			.unwrap()
			.get(&key(module))
			.and_then(|node| node.interface.clone())
	}

	/// Get the modules that a module imports.
	async fn module_imports(&self, module: &tg::module::Data) -> tg::Result<Vec<tg::module::Data>> {
		// Get the imports from the dependencies of a checked-in Python module, or otherwise from the module's source.
		let imports = if module.kind == tg::module::Kind::Python
			&& let tg::module::data::Source::Edge(_) = &module.referent.node
		{
			let graphs = crate::graphs::Graphs::default();
			let file = crate::python::resolve::module_file(&graphs, module)?;
			file.dependencies_with_instance(&self.instance)
				.await?
				.into_keys()
				.map(|reference| tg::module::Import {
					kind: None,
					reference,
				})
				.collect::<Vec<_>>()
		} else {
			let text = self.load_module(module).await?;
			Self::analyze(module, &text)?.imports.into_iter().collect()
		};

		// Resolve the imports concurrently.
		let modules = future::join_all(
			imports
				.into_iter()
				.map(|import| self.resolve_import(module, import)),
		)
		.await
		.into_iter()
		.filter_map(Result::ok)
		.filter(|module| language(module.kind).is_some())
		.collect();

		Ok(modules)
	}
}

impl Interfaces {
	fn loaded(&self, language: tg::module::load::Language) -> &Mutex<Loaded> {
		match language {
			tg::module::load::Language::JavaScript => &self.javascript,
			tg::module::load::Language::Python => &self.python,
		}
	}
}

/// Get the language of a module's checker.
fn language(kind: tg::module::Kind) -> Option<tg::module::load::Language> {
	match kind {
		tg::module::Kind::JavaScript
		| tg::module::Kind::TypeScript
		| tg::module::Kind::TypeScriptDeclaration => Some(tg::module::load::Language::JavaScript),
		tg::module::Kind::Python => Some(tg::module::load::Language::Python),
		_ => None,
	}
}

fn key(module: &tg::module::Data) -> Key {
	Key::new(module)
}

/// Get the modules in other languages that each module's checker loads, which are those it reaches through modules in its own language.
fn cross_language_imports(nodes: &BTreeMap<Key, Node>) -> BTreeMap<Key, BTreeSet<Key>> {
	// Create the graph of the imports between modules in the same language, and collect each module's imports of modules in other languages.
	let mut graph = DiGraph::<&Key, ()>::new();
	let indices = nodes
		.keys()
		.map(|key| (key, graph.add_node(key)))
		.collect::<BTreeMap<_, _>>();
	let mut direct = BTreeMap::<&Key, BTreeSet<Key>>::new();
	for (key, node) in nodes {
		let language = language(node.module.kind);
		for module in &node.imports {
			let import = self::key(module);
			let Some(&index) = indices.get(&import) else {
				continue;
			};
			if self::language(module.kind) == language {
				graph.add_edge(indices[key], index, ());
			} else {
				direct.entry(key).or_default().insert(import);
			}
		}
	}

	// Collect the imports of each group of mutually dependent modules after the groups that it imports.
	let condensation = petgraph::algo::condensation(graph, true);
	let order = petgraph::algo::toposort(&condensation, None)
		.unwrap_or_else(|_| unreachable!("a condensation is acyclic"));
	let mut imports = vec![BTreeSet::new(); condensation.node_count()];
	for index in order.into_iter().rev() {
		let mut set = BTreeSet::new();
		for key in &condensation[index] {
			set.extend(direct.get(key).into_iter().flatten().cloned());
		}
		for neighbor in condensation.neighbors(index) {
			set.extend(imports[neighbor.index()].iter().cloned());
		}
		imports[index.index()] = set;
	}

	// Give each module the imports of its group.
	let mut output = BTreeMap::new();
	for index in condensation.node_indices() {
		for key in &condensation[index] {
			output.insert((*key).clone(), imports[index.index()].clone());
		}
	}

	output
}

/// Get the strongly connected components that the targets reach, with each component after the components it depends on.
fn components<K>(dependencies: &BTreeMap<K, BTreeSet<K>>, targets: &BTreeSet<K>) -> Vec<Vec<K>>
where
	K: Clone + Ord,
{
	// Create the graph of the keys that the targets reach.
	let mut graph = DiGraph::<&K, ()>::new();
	let mut indices = BTreeMap::new();
	let mut pending = targets.iter().collect::<Vec<_>>();
	while let Some(key) = pending.pop() {
		if indices.contains_key(key) {
			continue;
		}
		indices.insert(key, graph.add_node(key));
		pending.extend(dependencies.get(key).into_iter().flatten());
	}
	for (key, index) in &indices {
		for dependency in dependencies.get(*key).into_iter().flatten() {
			graph.add_edge(*index, indices[dependency], ());
		}
	}

	// Get the components, which petgraph returns with each component after the components it depends on.
	petgraph::algo::tarjan_scc(&graph)
		.into_iter()
		.map(|component| {
			let mut component = component
				.into_iter()
				.map(|index| graph[index].clone())
				.collect::<Vec<_>>();
			component.sort();
			component
		})
		.collect()
}

/// Hash the versions of the modules that a module reaches.
fn fingerprint(nodes: &BTreeMap<Key, Node>, key: &Key) -> u64 {
	let mut reachable = BTreeSet::new();
	let mut pending = vec![key.clone()];
	while let Some(key) = pending.pop() {
		let Some(node) = nodes.get(&key) else {
			continue;
		};
		if !reachable.insert(key) {
			continue;
		}
		pending.extend(node.imports.iter().map(self::key));
	}
	let mut hasher = fnv::FnvHasher::default();
	for key in reachable {
		key.hash(&mut hasher);
		nodes[&key].version.hash(&mut hasher);
	}
	hasher.finish()
}

/// Hash an interface without the tokens of its imports, so that refreshed authorization does not change its version.
fn hash(interface: &tg::Interface) -> u64 {
	let mut interface = interface.clone();
	for module in interface.imports.values_mut() {
		*module = module.without_token();
	}
	let mut hasher = fnv::FnvHasher::default();
	serde_json::to_string(&interface).unwrap().hash(&mut hasher);
	hasher.finish()
}

#[cfg(test)]
mod tests {
	use std::collections::{BTreeMap, BTreeSet};

	fn dependencies(edges: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
		edges
			.iter()
			.map(|(key, dependencies)| {
				let dependencies = dependencies.iter().map(ToString::to_string).collect();
				(key.to_string(), dependencies)
			})
			.collect()
	}

	#[test]
	fn components_follow_dependencies() {
		let dependencies = dependencies(&[("a", &["b"]), ("b", &["c"]), ("c", &[])]);
		let targets = BTreeSet::from(["a".to_owned()]);
		let components = super::components(&dependencies, &targets);
		assert_eq!(components, vec![vec!["c"], vec!["b"], vec!["a"]]);
	}

	#[test]
	fn components_group_cycles() {
		let dependencies =
			dependencies(&[("a", &["b"]), ("b", &["c", "d"]), ("c", &["b"]), ("d", &[])]);
		let targets = BTreeSet::from(["a".to_owned()]);
		let components = super::components(&dependencies, &targets);
		assert_eq!(components, vec![vec!["d"], vec!["b", "c"], vec!["a"]]);
	}
}
