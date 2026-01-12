use {super::get_state, pyo3::prelude::*, tangram_client::prelude::*};

#[pyfunction]
pub fn magic(py: Python<'_>, function: PyObject) -> PyResult<PyObject> {
	let state = get_state(py)?;
	let function_ref = function.bind(py);

	// Get the function name.
	let name: Option<String> = function_ref
		.getattr("__name__")
		.ok()
		.and_then(|n| n.extract().ok());

	// Try to find the module this function belongs to.
	let file_name: Option<String> = function_ref
		.getattr("__module__")
		.ok()
		.and_then(|m| m.extract().ok());

	// Find the module in our modules list.
	let modules = state
		.modules
		.lock()
		.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("lock failed: {e}")))?;
	let mut found_module = None;

	if let Some(ref file) = file_name {
		// Try to match against loaded modules.
		for module_info in modules.iter() {
			let module_str = module_info.module.to_string();
			if file == &module_str || file.contains(&module_str) || file == "__tangram_root__" {
				found_module = Some(module_info.module.clone());
				break;
			}
		}
	}

	// If no module is found, then use the root module.
	let module = found_module.unwrap_or_else(|| state.root.clone());

	// Create the executable.
	let executable = tg::command::data::Executable::Module(tg::command::data::ModuleExecutable {
		module,
		export: name,
	});

	pythonize::pythonize(py, &executable)
		.map(pyo3::Bound::unbind)
		.map_err(|e| {
			pyo3::exceptions::PyRuntimeError::new_err(format!("failed to convert executable: {e}"))
		})
}
