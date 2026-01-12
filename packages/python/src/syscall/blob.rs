use {
	super::get_state,
	futures::TryStreamExt as _,
	pyo3::prelude::*,
	std::{io::Cursor, pin::pin},
	tangram_client::prelude::*,
	tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
};

#[pyfunction]
pub fn read(py: Python<'_>, arg: PyObject) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	let arg: tg::read::Arg = pythonize::depythonize(arg.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse read arg: {e}"))
	})?;

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		let buffer = main_runtime_handle
			.spawn(async move {
				let stream = handle.read(arg).await?;
				let reader = StreamReader::new(
					stream
						.map_ok(|chunk| chunk.bytes)
						.map_err(std::io::Error::other),
				);
				let mut buffer = Vec::new();
				pin!(reader)
					.read_to_end(&mut buffer)
					.await
					.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
				Ok::<_, tg::Error>(buffer)
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to read blob: {e}"))
			})?;

		Ok(buffer)
	})
}

#[pyfunction]
pub fn write(py: Python<'_>, bytes: Vec<u8>) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		let output = main_runtime_handle
			.spawn(async move {
				let reader = Cursor::new(bytes);
				handle.write(reader).await
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to write blob: {e}"))
			})?;

		Ok(output.blob.to_string())
	})
}
