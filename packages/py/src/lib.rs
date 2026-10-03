use {
	pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyModule},
	std::{
		collections::{BTreeMap, BTreeSet},
		ffi::CString,
		path::{Path, PathBuf},
		sync::{Mutex, mpsc},
	},
	tangram_client::prelude::*,
};

static RUN: Mutex<()> = Mutex::new(());

pub struct Arg {
	pub args: tg::value::data::Array,
	pub cwd: PathBuf,
	pub env: tg::value::data::Map,
	pub export: Option<String>,
	pub instance: tg::instance::dynamic::Instance,
	pub main_runtime_handle: tokio::runtime::Handle,
	pub module: tg::module::Data,
	pub token: Option<String>,
	pub url: String,
}

pub struct Outcome {
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

#[pyclass]
struct Host {
	instance: tg::instance::dynamic::Instance,
	main_runtime_handle: tokio::runtime::Handle,
}

pub fn run(arg: Arg) -> tg::Result<Outcome> {
	// Serialize invocations because CPython modules and process context are shared.
	let _guard = RUN
		.lock()
		.map_err(|_| tg::error!("the Python runtime lock is poisoned"))?;

	// Serialize the process context.
	let context = serde_json::json!({
		"args": arg.args,
		"cwd": arg.cwd,
		"env": arg.env,
		"export": arg.export,
		"module": arg.module,
		"token": arg.token,
		"url": arg.url,
	});
	let host = Host {
		instance: arg.instance,
		main_runtime_handle: arg.main_runtime_handle,
	};

	// Initialize and execute Python on the calling runtime thread.
	Python::initialize();
	let (exit, output, error) = Python::attach(|py| -> PyResult<_> {
		let native = PyModule::new(py, "tangram._native")?;
		tangram_py_native::_native(&native)?;
		py.import("sys")?
			.getattr("modules")?
			.set_item("tangram._native", native)?;
		let source = CString::new(include_str!("main.py")).unwrap();
		let filename = CString::new("<tangram main>").unwrap();
		let name = CString::new("_tangram_main").unwrap();
		let runtime = PyModule::from_code(py, &source, &filename, &name)?;
		runtime
			.getattr("run")?
			.call1((
				context.to_string(),
				include_str!(concat!(env!("OUT_DIR"), "/modules.json")),
				host,
			))?
			.extract::<(u8, Option<String>, Option<String>)>()
	})
	.map_err(|error| tg::error!(!error, "failed to execute the Python runtime"))?;

	// Deserialize the outcome using the shared Tangram codecs.
	let output = output
		.map(|output| {
			let data = serde_json::from_str(&output)
				.map_err(|error| tg::error!(!error, "failed to deserialize the output"))?;
			tg::Value::try_from_data(data)
		})
		.transpose()?;
	let error = error
		.map(|error| {
			let data = serde_json::from_str::<tg::error::Data>(&error)
				.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
			tg::Error::try_from(data)
		})
		.transpose()?;
	let outcome = Outcome {
		error,
		exit,
		output,
	};

	Ok(outcome)
}

#[pymethods]
impl Host {
	fn inventory(&self, py: Python<'_>, module: &str) -> PyResult<String> {
		let module = serde_json::from_str::<tg::module::Data>(module)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let result = load_inventory(instance, module).await;
				let _ = sender.send(result);
			}
		});
		py.detach(move || {
			receiver
				.recv()
				.map_err(|error| PyRuntimeError::new_err(error.to_string()))?
				.map_err(|error| PyRuntimeError::new_err(error.to_string()))
		})
	}

	fn module(
		&self,
		py: Python<'_>,
		referrer: &str,
		reference: Option<String>,
	) -> PyResult<(String, String)> {
		let module = serde_json::from_str::<tg::module::Data>(referrer)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let result = load_module(instance, module, reference).await;
				let _ = sender.send(result);
			}
		});
		py.detach(move || {
			receiver
				.recv()
				.map_err(|error| PyRuntimeError::new_err(error.to_string()))?
				.map_err(|error| PyRuntimeError::new_err(error.to_string()))
		})
	}
}

async fn load_module(
	instance: tg::instance::dynamic::Instance,
	mut module: tg::module::Data,
	reference: Option<String>,
) -> tg::Result<(String, String)> {
	// Resolve a relative import with its package's referrer.
	if let Some(reference) = reference {
		let import = tg::module::Import {
			kind: Some(tg::module::Kind::Py),
			reference: reference.parse()?,
		};
		let arg = tg::module::resolve::Arg {
			import,
			referrer: Some(module),
		};
		module = instance.resolve_module(arg).await?.module;
	}

	// Load the source through the existing module API.
	let arg = tg::module::load::Arg {
		module: module.clone(),
	};
	let text = instance.load_module(arg).await?.text;
	let data = serde_json::to_string(&module)
		.map_err(|error| tg::error!(!error, "failed to serialize the module"))?;

	Ok((data, text))
}

async fn load_inventory(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
) -> tg::Result<String> {
	if matches!(module.referent.node, tg::module::data::Source::Path(_)) {
		return Ok("null".to_owned());
	}
	let name = module
		.referent
		.path()
		.and_then(Path::file_name)
		.unwrap_or_else(|| std::ffi::OsStr::new("tangram.py"));
	let entry = Path::new("/tangram").join(name);
	let mut pending = vec![(entry.clone(), module)];
	let mut identities = BTreeMap::new();
	let mut modules = BTreeMap::new();
	let mut seen = BTreeSet::new();
	while let Some((path, module)) = pending.pop() {
		if !register_module_path(&mut identities, &path, &module)? {
			continue;
		}
		let arg = tg::module::load::Arg {
			module: module.clone(),
		};
		let text = instance.load_module(arg).await?.text;
		modules.insert(path.clone(), (module.clone(), text));
		let identity = module.without_token();
		if !seen.insert(identity) {
			continue;
		}
		let source = tg::Module::try_from_data(module.clone())?.referent.node;
		let tg::module::Source::Edge(edge) = source else {
			continue;
		};
		let file = match edge {
			tg::graph::Edge::Object(object) => object
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a Python module file"))?,
			tg::graph::Edge::Pointer(pointer) => {
				pointer
					.graph
					.state()
					.set_tokens(module.referent.options.tokens.clone());
				tg::Artifact::with_pointer(pointer)
					.try_unwrap_file()
					.map_err(|_| tg::error!("expected a Python module file"))?
			},
			tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
		};
		file.state()
			.set_tokens(module.referent.options.tokens.clone());
		for (reference, _) in file.dependencies_with_instance(&instance).await? {
			let Ok(relative) = reference.node().try_unwrap_path_ref() else {
				continue;
			};
			if tg::module::module_kind_for_path(relative).ok() != Some(tg::module::Kind::Py) {
				continue;
			}
			let mut filename = PathBuf::new();
			for component in path.parent().unwrap().join(relative).components() {
				match component {
					std::path::Component::CurDir => (),
					std::path::Component::ParentDir => {
						filename.pop();
					},
					component => filename.push(component),
				}
			}
			let import = tg::module::Import {
				kind: Some(tg::module::Kind::Py),
				reference,
			};
			let arg = tg::module::resolve::Arg {
				import,
				referrer: Some(module.clone()),
			};
			let dependency = instance.resolve_module(arg).await?.module;
			pending.push((filename, dependency));
		}
	}
	let inventory = serde_json::json!({"entry": entry, "modules": modules});
	let inventory = serde_json::to_string(&inventory)
		.map_err(|error| tg::error!(!error, "failed to serialize the Python modules"))?;
	Ok(inventory)
}

fn register_module_path(
	identities: &mut BTreeMap<PathBuf, tg::module::Data>,
	path: &Path,
	module: &tg::module::Data,
) -> tg::Result<bool> {
	if let Some(existing) = identities.get(path) {
		if !existing.has_same_identity(module) {
			return Err(tg::error!(
				"conflicting Python modules at the same path: {}",
				path.display()
			));
		}
		return Ok(false);
	}
	identities.insert(path.to_owned(), module.clone());
	Ok(true)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn python_module_paths_reject_conflicting_identities() {
		let path = Path::new("/tangram/helper.tg.py");
		let first = tg::module::Data {
			kind: tg::module::Kind::Py,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"/first/helper.tg.py".into(),
			)),
		};
		let second = tg::module::Data {
			kind: tg::module::Kind::Py,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"/second/helper.tg.py".into(),
			)),
		};
		let mut identities = BTreeMap::new();
		assert!(register_module_path(&mut identities, path, &first).unwrap());
		assert!(!register_module_path(&mut identities, path, &first).unwrap());
		let error = register_module_path(&mut identities, path, &second).unwrap_err();
		assert!(error.to_string().contains("conflicting Python modules"));
		assert_eq!(identities.get(path), Some(&first));
	}
}
