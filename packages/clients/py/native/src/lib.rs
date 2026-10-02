use {pyo3::prelude::*, tangram_client as tg};

#[pyfunction]
fn checksum(input: &[u8], algorithm: &str) -> PyResult<String> {
	let algorithm = algorithm
		.parse()
		.map_err(|error| to_python_error(&tg::error!(!error, "invalid checksum algorithm")))?;
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(input);
	let output = writer.finalize().to_string();
	Ok(output)
}

#[pyfunction]
fn normalize_object(input: &str) -> PyResult<String> {
	let data = serde_json::from_str::<tg::object::Data>(input).map_err(|error| {
		to_python_error(&tg::error!(!error, "failed to deserialize the object data"))
	})?;
	let output = serde_json::to_string(&data.without_location_and_tokens()).map_err(|error| {
		to_python_error(&tg::error!(!error, "failed to serialize the object data"))
	})?;
	Ok(output)
}

#[pyfunction]
fn object_id(input: &str) -> PyResult<String> {
	let data = serde_json::from_str::<tg::object::Data>(input).map_err(|error| {
		to_python_error(&tg::error!(!error, "failed to deserialize the object data"))
	})?;
	let data = data.without_location_and_tokens();
	let bytes = data.serialize().map_err(|error| to_python_error(&error))?;
	let id = tg::object::Id::new(data.kind(), &bytes);
	Ok(id.to_string())
}

#[pyfunction]
fn parse_value(input: &str) -> PyResult<String> {
	let value = input
		.parse::<tg::Value>()
		.map_err(|error| to_python_error(&error))?;
	let output = serde_json::to_string(&value.to_data()).map_err(|error| {
		to_python_error(&tg::error!(!error, "failed to serialize the value data"))
	})?;
	Ok(output)
}

#[pyfunction]
fn stringify_value(input: &str) -> PyResult<String> {
	let data = serde_json::from_str::<tg::value::Data>(input).map_err(|error| {
		to_python_error(&tg::error!(!error, "failed to deserialize the value data"))
	})?;
	let value = tg::Value::try_from_data(data).map_err(|error| to_python_error(&error))?;
	Ok(value.to_string())
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
	module.add_function(wrap_pyfunction!(checksum, module)?)?;
	module.add_function(wrap_pyfunction!(normalize_object, module)?)?;
	module.add_function(wrap_pyfunction!(object_id, module)?)?;
	module.add_function(wrap_pyfunction!(parse_value, module)?)?;
	module.add_function(wrap_pyfunction!(stringify_value, module)?)?;
	Ok(())
}

fn to_python_error(error: &tg::Error) -> PyErr {
	pyo3::exceptions::PyValueError::new_err(error.to_string())
}
