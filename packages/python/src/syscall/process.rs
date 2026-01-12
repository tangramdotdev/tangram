use {super::get_state, pyo3::prelude::*, tangram_client::prelude::*};

#[pyfunction]
pub fn process_get(py: Python<'_>, id: String) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	let id: tg::process::Id = id.parse().map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse process id: {e}"))
	})?;

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		let data = main_runtime_handle
			.spawn(async move {
				let tg::process::get::Output { data, .. } = handle.get_process(&id).await?;
				Ok::<_, tg::Error>(data)
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to get process: {e}"))
			})?;

		Python::with_gil(|py| {
			pythonize::pythonize(py, &data)
				.map(pyo3::Bound::unbind)
				.map_err(|e| {
					pyo3::exceptions::PyRuntimeError::new_err(format!(
						"failed to convert process data: {e}"
					))
				})
		})
	})
}

#[pyfunction]
pub fn process_spawn(py: Python<'_>, arg: PyObject) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	let arg: tg::process::spawn::Arg = pythonize::depythonize(arg.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse spawn arg: {e}"))
	})?;

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		let output = main_runtime_handle
			.spawn(async move { handle.spawn_process(arg).await })
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to spawn process: {e}"))
			})?;

		Python::with_gil(|py| {
			pythonize::pythonize(py, &output)
				.map(pyo3::Bound::unbind)
				.map_err(|e| {
					pyo3::exceptions::PyRuntimeError::new_err(format!(
						"failed to convert spawn output: {e}"
					))
				})
		})
	})
}

#[pyfunction]
pub fn process_wait(py: Python<'_>, id: String) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	let id: tg::process::Id = id.parse().map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse process id: {e}"))
	})?;

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		let output = main_runtime_handle
			.spawn(async move {
				let arg = tg::process::wait::Arg::default();
				handle.wait_process(&id, arg).await
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!(
					"failed to wait for process: {e}"
				))
			})?;

		Python::with_gil(|py| {
			pythonize::pythonize(py, &output)
				.map(pyo3::Bound::unbind)
				.map_err(|e| {
					pyo3::exceptions::PyRuntimeError::new_err(format!(
						"failed to convert wait output: {e}"
					))
				})
		})
	})
}
