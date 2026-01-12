use {crate::StateHandle, pyo3::prelude::*};

mod blob;
mod checksum;
mod encoding;
mod log;
mod magic;
mod object;
mod process;
mod sleep;

/// Register all syscall functions as a Python module.
pub fn register_syscalls(py: Python<'_>, state: StateHandle) -> PyResult<()> {
	let sys = py.import("sys")?;
	let modules = sys.getattr("modules")?;

	// Create the _tangram_syscall module.
	let syscall_module = PyModule::new(py, "_tangram_syscall")?;

	// Store state in the module for access by syscall functions.
	syscall_module.setattr("_state", Py::new(py, StateWrapper(state.clone()))?)?;

	// Add encoding functions.
	syscall_module.add_function(wrap_pyfunction!(encoding::base64_decode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::base64_encode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::hex_decode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::hex_encode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::json_decode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::json_encode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::toml_decode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::toml_encode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::utf8_decode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::utf8_encode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::yaml_decode, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(encoding::yaml_encode, &syscall_module)?)?;

	// Add log function.
	syscall_module.add_function(wrap_pyfunction!(log::log, &syscall_module)?)?;

	// Add object functions.
	syscall_module.add_function(wrap_pyfunction!(object::object_id, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(object::object_get, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(object::object_batch, &syscall_module)?)?;

	// Add process functions.
	syscall_module.add_function(wrap_pyfunction!(process::process_get, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(process::process_spawn, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(process::process_wait, &syscall_module)?)?;

	// Add blob functions.
	syscall_module.add_function(wrap_pyfunction!(blob::read, &syscall_module)?)?;
	syscall_module.add_function(wrap_pyfunction!(blob::write, &syscall_module)?)?;

	// Add checksum function.
	syscall_module.add_function(wrap_pyfunction!(checksum::checksum, &syscall_module)?)?;

	// Add sleep function.
	syscall_module.add_function(wrap_pyfunction!(sleep::sleep, &syscall_module)?)?;

	// Add magic function.
	syscall_module.add_function(wrap_pyfunction!(magic::magic, &syscall_module)?)?;

	// Register the module.
	modules.set_item("_tangram_syscall", syscall_module)?;

	Ok(())
}

/// Get the state from the module.
pub fn get_state(py: Python<'_>) -> PyResult<StateHandle> {
	let sys = py.import("sys")?;
	let modules = sys.getattr("modules")?;
	let syscall_module = modules.get_item("_tangram_syscall")?;
	if syscall_module.is_none() {
		return Err(pyo3::exceptions::PyRuntimeError::new_err(
			"_tangram_syscall module not found in sys.modules",
		));
	}
	let state_wrapper: Py<StateWrapper> = syscall_module.getattr("_state")?.extract()?;
	Ok(state_wrapper.borrow(py).0.clone())
}

/// Wrapper to store `StateHandle` in Python.
#[pyclass]
pub struct StateWrapper(pub(crate) StateHandle);
