use {super::get_state, pyo3::prelude::*, tangram_client::prelude::*};

/// Compute the ID for an object from its data. This is pure computation, so it stays synchronous.
#[pyfunction]
pub fn object_id(py: Python<'_>, data: PyObject) -> PyResult<String> {
	let data: tg::object::Data = pythonize::depythonize(data.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse object data: {e}"))
	})?;

	let bytes = data.serialize().map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to serialize object data: {e}"))
	})?;
	let id = tg::object::Id::new(data.kind(), &bytes);
	Ok(id.to_string())
}

#[pyfunction]
pub fn object_get(py: Python<'_>, id: String) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	let id: tg::object::Id = id.parse().map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse object id: {e}"))
	})?;
	let kind = id.kind();

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		let data = main_runtime_handle
			.spawn(async move {
				let arg = tg::object::get::Arg::default();
				let tg::object::get::Output { bytes } = handle.get_object(&id, arg).await?;
				let data = tg::object::Data::deserialize(kind, bytes)?;
				Ok::<_, tg::Error>(data)
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to get object: {e}"))
			})?;

		Python::with_gil(|py| {
			pythonize::pythonize(py, &data)
				.map(pyo3::Bound::unbind)
				.map_err(|e| {
					pyo3::exceptions::PyRuntimeError::new_err(format!(
						"failed to convert object: {e}"
					))
				})
		})
	})
}

/// Argument for object_batch - receives data dicts, serializes them in Rust.
#[derive(serde::Deserialize)]
struct BatchArg {
	objects: Vec<BatchObject>,
}

#[derive(serde::Deserialize)]
struct BatchObject {
	id: tg::object::Id,
	data: tg::object::Data,
}

#[pyfunction]
pub fn object_batch(py: Python<'_>, arg: PyObject) -> PyResult<Bound<'_, PyAny>> {
	let state = get_state(py)?;
	let handle = state.handle.clone();
	let main_runtime_handle = state.main_runtime_handle.clone();

	let arg: BatchArg = pythonize::depythonize(arg.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to parse batch arg: {e}"))
	})?;

	pyo3_async_runtimes::tokio::future_into_py(py, async move {
		main_runtime_handle
			.spawn(async move {
				if arg.objects.is_empty() {
					return Ok::<_, tg::Error>(());
				}
				let mut batch_objects = Vec::with_capacity(arg.objects.len());
				for object in arg.objects {
					let bytes = object
						.data
						.serialize()
						.map_err(|e| tg::error!(!e, "failed to serialize object"))?;
					batch_objects.push(tg::object::batch::Object {
						id: object.id,
						bytes,
					});
				}
				let batch_arg = tg::object::batch::Arg {
					objects: batch_objects,
					..Default::default()
				};
				handle.post_object_batch(batch_arg).await?;
				Ok(())
			})
			.await
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("task join failed: {e}"))
			})?
			.map_err(|e| {
				pyo3::exceptions::PyRuntimeError::new_err(format!("failed to batch objects: {e}"))
			})?;

		Ok(())
	})
}
