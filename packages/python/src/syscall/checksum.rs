use {pyo3::prelude::*, tangram_client::prelude::*};

#[pyfunction]
pub fn checksum(bytes: Vec<u8>, algorithm: String) -> PyResult<String> {
	let algorithm: tg::checksum::Algorithm = algorithm
		.parse()
		.map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("invalid algorithm: {e}")))?;

	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(&bytes);
	let checksum = writer.finalize();

	Ok(checksum.to_string())
}
