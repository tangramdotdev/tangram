use {super::get_state, pyo3::prelude::*, tangram_client as tg};

#[pyfunction]
pub fn log(py: Python<'_>, stream: String, message: String) -> PyResult<()> {
	let state = get_state(py)?;

	let stream = match stream.as_str() {
		"stdout" => tg::process::log::Stream::Stdout,
		"stderr" => tg::process::log::Stream::Stderr,
		_ => {
			return Err(pyo3::exceptions::PyValueError::new_err(
				"stream must be 'stdout' or 'stderr'",
			));
		},
	};

	// Spawn the log operation on the main runtime.
	let (sender, receiver) = std::sync::mpsc::channel();
	state.main_runtime_handle.spawn({
		let logger = state.logger.clone();
		async move {
			let result = logger(stream, message).await;
			let _ = sender.send(result);
		}
	});

	// Wait for the result.
	receiver
		.recv()
		.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("failed to log: {e}")))?
		.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("failed to log: {e}")))?;

	Ok(())
}
