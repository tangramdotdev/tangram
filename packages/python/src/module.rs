use {
	crate::{Module, StateHandle},
	pyo3::prelude::*,
	tangram_client::prelude::*,
};

/// Install the Tangram import hook into Python's `sys.meta_path`.
pub fn install_import_hook(py: Python<'_>, state: StateHandle) -> PyResult<()> {
	let sys = py.import("sys")?;
	let meta_path = sys.getattr("meta_path")?;

	// Create the finder and insert it at the beginning of meta_path.
	let finder = Py::new(py, TangramMetaFinder::new(state))?;
	meta_path.call_method1("insert", (0, finder))?;

	Ok(())
}

/// Import the root module.
pub fn import_root_module<'py>(
	py: Python<'py>,
	state: &StateHandle,
) -> PyResult<Bound<'py, PyAny>> {
	let root = &state.root;

	// Load the module source.
	let source = load_module_source(state, root)
		.map_err(|e| pyo3::exceptions::PyImportError::new_err(e.to_string()))?;

	// Create a new module.
	let types = py.import("types")?;
	let module_type = types.getattr("ModuleType")?;
	let module = module_type.call1(("__tangram_root__",))?;

	// Set module attributes.
	module.setattr("__file__", root.to_string())?;
	module.setattr("__loader__", py.None())?;
	module.setattr("__package__", "")?;

	// Compile and execute the module code.
	let code = py.import("builtins")?.getattr("compile")?;
	let compiled = code.call1((&source, root.to_string(), "exec"))?;

	let exec = py.import("builtins")?.getattr("exec")?;
	exec.call1((compiled, module.getattr("__dict__")?))?;

	Ok(module)
}

/// Resolve a module import.
fn resolve_module(
	state: &StateHandle,
	referrer: &tg::module::Data,
	name: &str,
) -> tg::Result<tg::module::Data> {
	// Parse the import specifier.
	let import = tg::module::Import::with_specifier_and_attributes(name, None)?;

	// Resolve the module using the handle.
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let handle = state.handle.clone();
		let referrer = referrer.clone();
		let import = import.clone();
		async move {
			let arg = tg::module::resolve::Arg { referrer, import };
			let result = handle.resolve_module(arg).await.map(|output| output.module);
			sender.send(result).unwrap();
		}
	});

	receiver.recv().unwrap()
}

/// Load a module's source code.
fn load_module_source(state: &StateHandle, module: &tg::module::Data) -> tg::Result<String> {
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let handle = state.handle.clone();
		let module = module.clone();
		async move {
			let arg = tg::module::load::Arg { module };
			let result = handle.load_module(arg).await.map(|o| o.text);
			sender.send(result).unwrap();
		}
	});

	receiver.recv().unwrap()
}

/// Python meta path finder for Tangram modules.
#[pyclass(unsendable)]
pub struct TangramMetaFinder {
	state: StateHandle,
}

impl TangramMetaFinder {
	fn new(state: StateHandle) -> Self {
		Self { state }
	}
}

#[pymethods]
impl TangramMetaFinder {
	/// Find a module spec for the given module name.
	#[pyo3(signature = (fullname, path=None, target=None))]
	fn find_spec(
		&self,
		py: Python<'_>,
		fullname: &str,
		path: Option<PyObject>,
		target: Option<PyObject>,
	) -> PyResult<Option<PyObject>> {
		let _ = (path, target);

		// Only handle Tangram module imports.
		if !fullname.starts_with("tangram") && !fullname.starts_with('.') {
			return Ok(None);
		}

		// Try to resolve the module.
		let referrer = self.state.root.clone();
		let Ok(module_data) = resolve_module(&self.state, &referrer, fullname) else {
			return Ok(None);
		};

		// Create a ModuleSpec.
		let importlib_util = py.import("importlib.util")?;
		let loader = Py::new(
			py,
			TangramLoader::new(self.state.clone(), module_data.clone()),
		)?;
		let spec = importlib_util.call_method1("spec_from_loader", (fullname, loader))?;

		Ok(Some(spec.into()))
	}
}

/// Python loader for Tangram modules.
#[pyclass(unsendable)]
pub struct TangramLoader {
	state: StateHandle,
	module_data: tg::module::Data,
}

impl TangramLoader {
	fn new(state: StateHandle, module_data: tg::module::Data) -> Self {
		Self { state, module_data }
	}
}

#[pymethods]
impl TangramLoader {
	/// Create the module object. Return None to use default semantics.
	fn create_module(&self, _spec: PyObject) -> Option<PyObject> {
		None
	}

	/// Execute the module.
	fn exec_module(&self, py: Python<'_>, module: PyObject) -> PyResult<()> {
		// Load the source.
		let source = load_module_source(&self.state, &self.module_data)
			.map_err(|e| pyo3::exceptions::PyImportError::new_err(e.to_string()))?;

		// Register this module in our state.
		self.state
			.modules
			.lock()
			.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
			.push(Module {
				module: self.module_data.clone(),
			});

		// Set module attributes.
		let module_ref = module.bind(py);
		module_ref.setattr("__file__", self.module_data.to_string())?;
		module_ref.setattr("__tangram_module__", self.module_data.to_string())?;

		// Compile and execute the source.
		let builtins = py.import("builtins")?;
		let compile = builtins.getattr("compile")?;
		let exec = builtins.getattr("exec")?;

		let compiled = compile.call1((&source, self.module_data.to_string(), "exec"))?;
		exec.call1((compiled, module_ref.getattr("__dict__")?))?;

		Ok(())
	}

	/// Get the source code for a module.
	fn get_source(&self, _fullname: &str) -> PyResult<Option<String>> {
		match load_module_source(&self.state, &self.module_data) {
			Ok(source) => Ok(Some(source)),
			Err(_) => Ok(None),
		}
	}

	/// Check if this is a package.
	fn is_package(&self, _fullname: &str) -> bool {
		false
	}
}
