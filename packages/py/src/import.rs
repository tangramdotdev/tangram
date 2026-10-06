use {
	pyo3::{
		prelude::*,
		types::{PyBytes, PyModule},
	},
	rust_embed::Embed,
	std::{
		borrow::Cow,
		ffi::{CStr, CString, c_char},
		sync::OnceLock,
	},
	tangram_client::prelude::*,
};

#[derive(Embed)]
#[folder = "$TANGRAM_PYTHON_LIBRARY"]
struct Library;

static INITIALIZED: OnceLock<Result<(), String>> = OnceLock::new();

unsafe extern "C" {
	fn tangram_python_initialize(error: *mut c_char, capacity: usize) -> i32;
}

pub fn initialize() -> tg::Result<()> {
	let result = INITIALIZED.get_or_init(|| {
		let mut error = [0 as c_char; 1024];
		// SAFETY: OnceLock serializes initialization, and the writable error buffer has the supplied capacity.
		let status = unsafe { tangram_python_initialize(error.as_mut_ptr(), error.len()) };
		if status != 0 {
			// SAFETY: The initialization function always NUL-terminates the error buffer on failure.
			let message = unsafe { CStr::from_ptr(error.as_ptr()) }
				.to_string_lossy()
				.into_owned();
			return Err(message);
		}
		Python::attach(|py| -> PyResult<()> {
			let assets = PyModule::new(py, "_tangram_embedded")?;
			assets.add_function(wrap_pyfunction!(get, &assets)?)?;
			assets.add_function(wrap_pyfunction!(names, &assets)?)?;
			let source = CString::new(include_str!("import.py")).unwrap();
			let filename = CString::new("<tangram import>").unwrap();
			let name = CString::new("_tangram_import").unwrap();
			let module = PyModule::from_code(py, &source, &filename, &name)?;
			module.getattr("install")?.call1((assets,))?;
			Ok(())
		})
		.map_err(|error| error.to_string())
	});
	result
		.as_ref()
		.map_err(|message| tg::error!("failed to initialize the python runtime: {message}"))?;
	Ok(())
}

#[pyfunction]
fn get<'py>(py: Python<'py>, path: &str) -> Option<Bound<'py, PyBytes>> {
	Library::get(path).map(|file| PyBytes::new(py, &file.data))
}

#[pyfunction]
fn names() -> Vec<String> {
	Library::iter().map(Cow::into_owned).collect()
}
