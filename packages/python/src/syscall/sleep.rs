use {super::get_state, pyo3::prelude::*, std::time::Duration};

#[pyfunction]
pub fn sleep(py: Python<'_>, duration: f64) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let main_runtime_handle = state.main_runtime_handle.clone();
	let duration = Duration::from_secs_f64(duration);

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		main_runtime_handle
			.spawn(async move {
				tokio::time::sleep(duration).await;
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to sleep: {e}"))
			})?;

		Ok(())
	})
}
