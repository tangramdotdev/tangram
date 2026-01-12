use pyo3::prelude::*;

#[pyfunction]
pub fn base64_decode(value: String) -> PyResult<Vec<u8>> {
	data_encoding::BASE64.decode(value.as_bytes()).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to decode base64: {e}"))
	})
}

#[pyfunction]
pub fn base64_encode(value: Vec<u8>) -> String {
	data_encoding::BASE64.encode(&value)
}

#[pyfunction]
pub fn hex_decode(value: String) -> PyResult<Vec<u8>> {
	data_encoding::HEXLOWER
		.decode(value.as_bytes())
		.map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("failed to decode hex: {e}")))
}

#[pyfunction]
pub fn hex_encode(value: Vec<u8>) -> String {
	data_encoding::HEXLOWER.encode(&value)
}

#[pyfunction]
pub fn json_decode(py: Python<'_>, value: String) -> PyResult<PyObject> {
	let parsed: serde_json::Value = serde_json::from_str(&value).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to decode json: {e}"))
	})?;
	pythonize::pythonize(py, &parsed)
		.map(pyo3::Bound::unbind)
		.map_err(|e| {
			pyo3::exceptions::PyValueError::new_err(format!(
				"failed to convert json to python: {e}"
			))
		})
}

#[pyfunction]
pub fn json_encode(py: Python<'_>, value: PyObject) -> PyResult<String> {
	let parsed: serde_json::Value = pythonize::depythonize(value.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to convert python to json: {e}"))
	})?;
	serde_json::to_string(&parsed)
		.map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("failed to encode json: {e}")))
}

#[pyfunction]
pub fn toml_decode(py: Python<'_>, value: String) -> PyResult<PyObject> {
	let parsed: toml::Value = toml::from_str(&value).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to decode toml: {e}"))
	})?;
	pythonize::pythonize(py, &parsed)
		.map(pyo3::Bound::unbind)
		.map_err(|e| {
			pyo3::exceptions::PyValueError::new_err(format!(
				"failed to convert toml to python: {e}"
			))
		})
}

#[pyfunction]
pub fn toml_encode(py: Python<'_>, value: PyObject) -> PyResult<String> {
	let parsed: toml::Value = pythonize::depythonize(value.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to convert python to toml: {e}"))
	})?;
	toml::to_string(&parsed)
		.map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("failed to encode toml: {e}")))
}

#[pyfunction]
pub fn utf8_decode(value: Vec<u8>) -> PyResult<String> {
	String::from_utf8(value)
		.map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("failed to decode utf8: {e}")))
}

#[pyfunction]
pub fn utf8_encode(value: String) -> Vec<u8> {
	value.into_bytes()
}

#[pyfunction]
pub fn yaml_decode(py: Python<'_>, value: String) -> PyResult<PyObject> {
	let parsed: serde_yaml::Value = serde_yaml::from_str(&value).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to decode yaml: {e}"))
	})?;
	pythonize::pythonize(py, &parsed)
		.map(pyo3::Bound::unbind)
		.map_err(|e| {
			pyo3::exceptions::PyValueError::new_err(format!(
				"failed to convert yaml to python: {e}"
			))
		})
}

#[pyfunction]
pub fn yaml_encode(py: Python<'_>, value: PyObject) -> PyResult<String> {
	let parsed: serde_yaml::Value = pythonize::depythonize(value.bind(py)).map_err(|e| {
		pyo3::exceptions::PyValueError::new_err(format!("failed to convert python to yaml: {e}"))
	})?;
	serde_yaml::to_string(&parsed)
		.map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("failed to encode yaml: {e}")))
}
