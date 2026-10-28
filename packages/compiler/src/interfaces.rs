use {
	crate::Compiler,
	std::{
		collections::{BTreeMap, BTreeSet},
		hash::{Hash as _, Hasher as _},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
};

/// The graph of the modules that the checkers reach, with the interface of each module that a checker loads from another language.
#[derive(Default)]
pub(crate) struct Interfaces {
	/// The lock that serializes preparing interfaces.
	lock: tokio::sync::Mutex<()>,

	/// The modules, keyed by their identities.
	nodes: Mutex<BTreeMap<String, Node>>,
}

#[derive(Clone)]
struct Node {
	imports: Vec<tg::module::Data>,
	interface: Option<Arc<Entry>>,
	module: tg::module::Data,
	version: u64,
}

/// An interface that has been extracted for a module.
pub(crate) struct Entry {
	/// The hash of the versions of the modules that the module reaches when its interface was extracted.
	fingerprint: u64,

	/// The interface.
	pub(crate) interface: tg::Interface,

	/// The hash of the interface.
	pub(crate) version: u64,
}

impl Compiler {
	/// Extract the interfaces of the modules in other languages that the checker of the roots loads, in dependency order, so that no extraction waits on another checker.
	pub(crate) async fn prepare_interfaces(&self, roots: Vec<tg::module::Data>) {
		let _guard = self.interfaces.lock.lock().await;

		// Discover the modules that the roots reach.
		let mut nodes = BTreeMap::new();
		let mut pending = roots.clone();
		while let Some(module) = pending.pop() {
			if language(module.kind).is_none() {
				continue;
			}
			let key = key(&module);
			if nodes.contains_key(&key) {
				continue;
			}
			let version = self.get_module_version(&module).await.unwrap_or_default();
			let cached = self.interfaces.nodes.lock().unwrap().get(&key).cloned();
			let node = match cached {
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
			};
			pending.extend(node.imports.iter().cloned());
			nodes.insert(key, node);
		}

		// Find the interfaces that each module's checker loads from other languages.
		let dependencies = nodes
			.keys()
			.map(|key| (key.clone(), cross_language_imports(&nodes, key)))
			.collect::<BTreeMap<_, _>>();
		let targets = roots
			.iter()
			.filter_map(|module| dependencies.get(&key(module)))
			.flatten()
			.cloned()
			.collect::<BTreeSet<_>>();

		// Extract the interfaces of each group of mutually dependent modules after the groups they depend on.
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
				let module = &nodes[key].module;
				let interface = match self.extract_interface(module).await {
					Ok(interface) => interface,
					Err(error) => {
						tracing::debug!(error = %error.trace(), "failed to extract the interface");
						continue;
					},
				};
				let entry = Entry {
					fingerprint: fingerprints[key],
					version: hash(&interface),
					interface,
				};
				entries.push((key.clone(), Arc::new(entry)));
			}
			let mut shared = self.interfaces.nodes.lock().unwrap();
			for (key, entry) in entries {
				let node = nodes.get_mut(&key).unwrap();
				node.interface = Some(entry);
				shared.insert(key, node.clone());
			}
		}

		// Store the modules, and remove the modules that neither the roots nor the open documents reach.
		let documents = self.list_documents().await;
		let mut shared = self.interfaces.nodes.lock().unwrap();
		shared.extend(nodes);
		let mut reachable = BTreeSet::new();
		let mut pending = roots.iter().chain(&documents).map(key).collect::<Vec<_>>();
		while let Some(key) = pending.pop() {
			if !reachable.insert(key.clone()) {
				continue;
			}
			if let Some(node) = shared.get(&key) {
				pending.extend(node.imports.iter().map(self::key));
			}
		}
		shared.retain(|key, _| reachable.contains(key));
	}

	/// Get the extracted interface of a module, if there is one.
	pub(crate) fn try_get_interface(&self, module: &tg::module::Data) -> Option<Arc<Entry>> {
		self.interfaces
			.nodes
			.lock()
			.unwrap()
			.get(&key(module))
			.and_then(|node| node.interface.clone())
	}

	/// Extract the interface of a module.
	async fn extract_interface(&self, module: &tg::module::Data) -> tg::Result<tg::Interface> {
		let text = self.load_module(module).await?;
		crate::interface::module(module, &text)
	}

	/// Get the modules that a module imports.
	async fn module_imports(&self, module: &tg::module::Data) -> tg::Result<Vec<tg::module::Data>> {
		// Get the imports from the dependencies of a checked-in Python module, or otherwise from the module's source.
		let imports = match (&module.kind, &module.referent.node) {
			(tg::module::Kind::Python, tg::module::data::Source::Edge(_)) => {
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
			},
			_ => {
				let text = self.load_module(module).await?;
				Self::analyze(module, &text)?
					.imports
					.into_iter()
					.collect()
			},
		};

		// Resolve the imports.
		let mut modules = Vec::new();
		for import in imports {
			let arg = tg::module::resolve::Arg {
				import,
				referrer: Some(module.clone()),
			};
			let Ok(output) = self.instance.resolve_module(arg).await else {
				continue;
			};
			if language(output.module.kind).is_some() {
				modules.push(output.module);
			}
		}

		Ok(modules)
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

fn key(module: &tg::module::Data) -> String {
	module.without_token().to_string()
}

/// Get the modules in other languages that a module's checker loads, which are those it reaches through modules in its own language.
fn cross_language_imports(nodes: &BTreeMap<String, Node>, key: &str) -> BTreeSet<String> {
	let language = language(nodes[key].module.kind);
	let mut imports = BTreeSet::new();
	let mut visited = BTreeSet::new();
	let mut pending = vec![key.to_owned()];
	while let Some(key) = pending.pop() {
		if !visited.insert(key.clone()) {
			continue;
		}
		for module in &nodes[&key].imports {
			let import = self::key(module);
			if !nodes.contains_key(&import) {
				continue;
			}
			if self::language(module.kind) == language {
				pending.push(import);
			} else {
				imports.insert(import);
			}
		}
	}
	imports
}

/// Get the strongly connected components that the targets reach, with each component after the components it depends on.
fn components(
	dependencies: &BTreeMap<String, BTreeSet<String>>,
	targets: &BTreeSet<String>,
) -> Vec<Vec<String>> {
	struct State<'a> {
		components: Vec<Vec<String>>,
		dependencies: &'a BTreeMap<String, BTreeSet<String>>,
		indices: BTreeMap<&'a str, (usize, usize)>,
		next: usize,
		stack: Vec<&'a str>,
		stacked: BTreeSet<&'a str>,
	}

	fn visit<'a>(state: &mut State<'a>, key: &'a str) {
		let index = state.next;
		state.next += 1;
		state.indices.insert(key, (index, index));
		state.stack.push(key);
		state.stacked.insert(key);
		for dependency in &state.dependencies[key] {
			if !state.indices.contains_key(dependency.as_str()) {
				visit(state, dependency);
				let low = state.indices[dependency.as_str()].1;
				let entry = state.indices.get_mut(key).unwrap();
				entry.1 = entry.1.min(low);
			} else if state.stacked.contains(dependency.as_str()) {
				let low = state.indices[dependency.as_str()].0;
				let entry = state.indices.get_mut(key).unwrap();
				entry.1 = entry.1.min(low);
			}
		}
		let (index, low) = state.indices[key];
		if index == low {
			let mut component = Vec::new();
			loop {
				let member = state.stack.pop().unwrap();
				state.stacked.remove(member);
				component.push(member.to_owned());
				if member == key {
					break;
				}
			}
			component.sort();
			state.components.push(component);
		}
	}

	let mut state = State {
		components: Vec::new(),
		dependencies,
		indices: BTreeMap::new(),
		next: 0,
		stack: Vec::new(),
		stacked: BTreeSet::new(),
	};
	for target in targets {
		if !state.indices.contains_key(target.as_str()) {
			visit(&mut state, target);
		}
	}
	state.components
}

/// Hash the versions of the modules that a module reaches.
fn fingerprint(nodes: &BTreeMap<String, Node>, key: &str) -> u64 {
	let mut reachable = BTreeSet::new();
	let mut pending = vec![key.to_owned()];
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

fn hash(interface: &tg::Interface) -> u64 {
	let mut hasher = fnv::FnvHasher::default();
	serde_json::to_string(interface).unwrap().hash(&mut hasher);
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
		let dependencies = dependencies(&[
			("a", &["b"]),
			("b", &["c", "d"]),
			("c", &["b"]),
			("d", &[]),
		]);
		let targets = BTreeSet::from(["a".to_owned()]);
		let components = super::components(&dependencies, &targets);
		assert_eq!(components, vec![vec!["d"], vec!["b", "c"], vec!["a"]]);
	}
}
