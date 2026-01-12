use {pyo3::prelude::*, tangram_client::prelude::*};

/// Convert a Python exception to a Tangram error.
#[expect(dead_code)]
pub fn from_py_err(py: Python<'_>, err: &PyErr) -> tg::Error {
	let value = err.value(py);
	let message = value
		.str()
		.map_or_else(|_| "unknown Python error".to_owned(), |s| s.to_string());

	// Try to get the traceback.
	let traceback = err.traceback(py).and_then(|tb| {
		tb.call_method0("format")
			.ok()
			.and_then(|frames| frames.extract::<String>().ok())
	});

	if let Some(tb) = traceback {
		tg::error!("{message}\n{tb}")
	} else {
		tg::error!("{message}")
	}
}

/// Convert a Tangram error to a Python exception.
#[expect(dead_code)]
pub fn to_py_err(error: &tg::Error) -> PyErr {
	pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}
