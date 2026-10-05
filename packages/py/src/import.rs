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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn embedded_python_library_is_self_contained() {
		assert!(Library::get("encodings/__init__.py").is_none());
		let source = Library::get("os.py").unwrap();
		let compressed = Library::compressed("os.py").unwrap();
		assert!(compressed.data.compressed().len() < source.data.len());
		initialize().unwrap();
		Python::attach(|py| {
			py.run(c"import _imp, asyncio, bz2, ctypes, email, hashlib, importlib.resources, inspect, json, lzma, sqlite3, ssl, sys, sysconfig, zlib\nassert sys.path == []\nassert sys.prefix == '/tangram/python'\nassert sys.flags.isolated\nassert _imp.is_frozen('encodings')\nassert ssl._ssl.__spec__.origin == 'built-in'\nassert zlib.__spec__.origin == 'built-in'\nassert zlib.decompress(zlib.compress(b'hello')) == b'hello'\nassert bz2.decompress(bz2.compress(b'hello')) == b'hello'\nassert lzma.decompress(lzma.compress(b'hello')) == b'hello'\nassert hashlib.sha256(b'hello').hexdigest() == '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'\nassert sqlite3.connect(':memory:').execute('select 42').fetchone() == (42,)\nassert asyncio.run(asyncio.sleep(0, result=42)) == 42\nassert importlib.resources.files(email).joinpath('__init__.py').read_bytes()\nassert 'def dumps(' in inspect.getsource(json.dumps)\nassert sysconfig.get_config_var('Py_GIL_DISABLED') == 0\n", None, None).unwrap();
		});
	}
}
