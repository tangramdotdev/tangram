use {
	pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyModule},
	std::{
		ffi::CString,
		path::{Path, PathBuf},
		sync::{Mutex, mpsc},
	},
	tangram_client::prelude::*,
	tangram_compiler::py::resolve::prepare_module,
};

mod import;

static RUN: Mutex<()> = Mutex::new(());

pub struct Arg {
	pub args: tg::value::data::Array,
	pub cwd: PathBuf,
	pub env: tg::value::data::Map,
	pub export: Option<String>,
	pub instance: tg::instance::dynamic::Instance,
	pub main_runtime_handle: tokio::runtime::Handle,
	pub module: tg::module::Data,
	pub token: Option<String>,
	pub url: String,
}

pub struct Outcome {
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

#[pyclass]
struct Host {
	instance: tg::instance::dynamic::Instance,
	main_runtime_handle: tokio::runtime::Handle,
}

pub fn run(arg: Arg) -> tg::Result<Outcome> {
	// Serialize invocations because CPython modules and process context are shared.
	let _guard = RUN
		.lock()
		.map_err(|_| tg::error!("the python runtime lock is poisoned"))?;

	// Preserve the entry's graph pointer before creating the process context and module cache.
	let module = arg
		.main_runtime_handle
		.block_on(prepare_module(&arg.instance, arg.module))
		.map_err(|error| tg::error!(!error, "failed to prepare the python entry module"))?;

	// Serialize the process context.
	let context = serde_json::json!({
		"args": arg.args,
		"cwd": arg.cwd,
		"env": arg.env,
		"export": arg.export,
		"module": module,
		"token": arg.token,
		"url": arg.url,
	});
	let host = Host {
		instance: arg.instance,
		main_runtime_handle: arg.main_runtime_handle,
	};

	// Initialize and execute Python on the calling runtime thread.
	self::import::initialize()?;
	let (exit, output, error) = Python::attach(|py| -> PyResult<_> {
		let native = PyModule::new(py, "tangram._native")?;
		tangram_py_native::_native(&native)?;
		py.import("sys")?
			.getattr("modules")?
			.set_item("tangram._native", native)?;
		let source = CString::new(include_str!("main.py")).unwrap();
		let filename = CString::new("<tangram main>").unwrap();
		let name = CString::new("_tangram_main").unwrap();
		let runtime = PyModule::from_code(py, &source, &filename, &name)?;
		runtime
			.getattr("run")?
			.call1((context.to_string(), host))?
			.extract::<(u8, Option<String>, Option<String>)>()
	})
	.map_err(|error| tg::error!(!error, "failed to execute the python runtime"))?;

	// Deserialize the outcome using the shared Tangram codecs.
	let output = output
		.map(|output| {
			let data = serde_json::from_str(&output)
				.map_err(|error| tg::error!(!error, "failed to deserialize the output"))?;
			tg::Value::try_from_data(data)
		})
		.transpose()?;
	let error = error
		.map(|error| {
			let data = serde_json::from_str::<tg::error::Data>(&error)
				.map_err(|error| tg::error!(!error, "failed to deserialize the error"))?;
			tg::Error::try_from(data)
		})
		.transpose()?;
	let outcome = Outcome {
		error,
		exit,
		output,
	};

	Ok(outcome)
}

#[pymethods]
impl Host {
	#[staticmethod]
	fn describe(module: &str) -> PyResult<String> {
		let module = serde_json::from_str::<tg::module::Data>(module)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		serialize_module(module).map_err(|error| PyRuntimeError::new_err(error.to_string()))
	}

	fn resolve(&self, py: Python<'_>, request: &str) -> PyResult<String> {
		let request = serde_json::from_str::<tangram_compiler::py::resolve::Request>(request)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let resolver = tangram_compiler::py::resolve::Resolver::new(instance);
				let result = resolver.resolve(request).await.and_then(|output| {
					serde_json::to_string(&output).map_err(|error| {
						tg::error!(!error, "failed to serialize the python resolution")
					})
				});
				let _ = sender.send(result);
			}
		});
		let result = py
			.detach(move || receiver.recv())
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		result.map_err(|error| to_exception(py, &error))
	}

	fn load(&self, py: Python<'_>, module: &str) -> PyResult<String> {
		let module = serde_json::from_str::<tg::module::Data>(module)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let result = load_module(instance, module).await;
				let _ = sender.send(result);
			}
		});
		let result = py
			.detach(move || receiver.recv())
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		result.map_err(|error| to_exception(py, &error))
	}

	#[staticmethod]
	fn metadata(filename: &str, text: &str) -> String {
		let value = match tangram_compiler::analyze::py::metadata::parse(Path::new(filename), text)
		{
			Ok(metadata) => serde_json::to_value(metadata).unwrap(),
			Err(error) => serde_json::json!({"error": error.to_data_or_id().unwrap_left()}),
		};
		value.to_string()
	}
}

fn to_exception(py: Python<'_>, error: &tg::Error) -> PyErr {
	let result = (|| -> PyResult<_> {
		let data = match error.to_data_or_id() {
			tg::Either::Left(data) => serde_json::to_value(data).unwrap(),
			tg::Either::Right(_) => serde_json::json!(error.to_referent().to_string()),
		};
		let data = py
			.import("json")?
			.call_method1("loads", (data.to_string(),))?;
		py.import("tangram")?
			.getattr("Error")?
			.call_method1("from_data", (data,))
	})();
	match result {
		Ok(exception) => PyErr::from_value(exception),
		Err(error) => error,
	}
}

fn serialize_module(module: tg::module::Data) -> tg::Result<String> {
	let resolved = tangram_compiler::py::resolve::Module::new(module);
	let output = serde_json::to_string(&resolved)
		.map_err(|error| tg::error!(!error, "failed to serialize the python module"))?;
	Ok(output)
}

async fn load_module(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
) -> tg::Result<String> {
	let arg = tg::module::load::Arg {
		language: Some(tg::module::load::Language::Py),
		module,
	};
	let output = instance.load_module(arg).await?;
	let output = serde_json::to_string(&output)
		.map_err(|error| tg::error!(!error, "failed to serialize the python module load output"))?;
	Ok(output)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn module_identities_use_the_js_canonical_representation() {
		let mut keys = Vec::new();
		for options in [
			serde_json::json!(null),
			serde_json::json!({}),
			serde_json::json!({"path": null}),
		] {
			let value = serde_json::json!({"kind": "py", "referent": {"node": "/test/main.tg.py", "options": options}});
			let value = if value["referent"]["options"].is_null() {
				serde_json::json!({"kind": "py", "referent": {"node": "/test/main.tg.py"}})
			} else {
				value
			};
			let module = serde_json::from_value::<tg::module::Data>(value).unwrap();
			let key = module.without_token().to_string();
			let output = serialize_module(module).unwrap();
			let output = serde_json::from_str::<serde_json::Value>(&output).unwrap();
			assert_eq!(output["key"], key);
			keys.push(key);
		}
		assert!(keys.windows(2).all(|pair| pair[0] == pair[1]));
	}

	#[test]
	fn module_files_preserve_remote_regions() {
		let location = tg::Location::Remote(tg::location::Remote {
			name: "tools".to_owned(),
			region: Some("west".to_owned()),
		});
		let pointer = tg::graph::data::Pointer {
			graph: tg::graph::Id::new(b"graph"),
			index: 0,
			kind: tg::artifact::Kind::File,
		};
		let edges = [
			tg::graph::data::Edge::Object(tg::file::Id::new(b"file").into()),
			tg::graph::data::Edge::Pointer(pointer),
		];
		for edge in edges {
			let options = tg::referent::Options {
				location: Some(location.clone()),
				..Default::default()
			};
			let referent = tg::Referent::new(tg::module::data::Source::Edge(edge), options);
			let module = tg::module::Data {
				kind: tg::module::Kind::Py,
				referent,
			};
			let file = tangram_compiler::py::resolve::module_file(&module).unwrap();
			assert_eq!(file.state().location(), Some(location.clone()));
			if let Some(object) = file.state().object() {
				let tg::file::Object::Pointer(pointer) = object.unwrap_file().as_ref().clone()
				else {
					panic!("expected a graph-backed file");
				};
				assert_eq!(pointer.graph.state().location(), Some(location.clone()));
			}
		}
	}
}
