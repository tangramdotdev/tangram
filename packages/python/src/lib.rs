use {
	pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyModule},
	std::{
		ffi::CString,
		path::{Path, PathBuf},
		sync::{Mutex, mpsc},
	},
	tangram_client::prelude::*,
	tangram_compiler::python::resolve::prepare_module,
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
	let (exit, output, error) = Python::attach(|python| -> PyResult<_> {
		let native = PyModule::new(python, "tangram._native")?;
		tangram_python_native::_native(&native)?;
		python
			.import("sys")?
			.getattr("modules")?
			.set_item("tangram._native", native)?;
		let source = CString::new(include_str!("main.py")).unwrap();
		let filename = CString::new("<tangram main>").unwrap();
		let name = CString::new("_tangram_main").unwrap();
		let runtime = PyModule::from_code(python, &source, &filename, &name)?;
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

	fn resolve(&self, python: Python<'_>, request: &str) -> PyResult<String> {
		let request = serde_json::from_str::<tangram_compiler::python::resolve::Request>(request)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let resolver = tangram_compiler::python::resolve::Resolver::new(instance);
				let result = resolver.resolve(request).await.and_then(|output| {
					serde_json::to_string(&output).map_err(|error| {
						tg::error!(!error, "failed to serialize the python resolution")
					})
				});
				let _ = sender.send(result);
			}
		});
		let result = python
			.detach(move || receiver.recv())
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		result.map_err(|error| to_exception(python, &error))
	}

	fn load(&self, python: Python<'_>, module: &str) -> PyResult<String> {
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
		let result = python
			.detach(move || receiver.recv())
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		result.map_err(|error| to_exception(python, &error))
	}

	#[staticmethod]
	fn metadata(filename: &str, text: &str) -> String {
		let value =
			match tangram_compiler::analyze::python::metadata::parse(Path::new(filename), text) {
				Ok(metadata) => serde_json::to_value(metadata).unwrap(),
				Err(error) => serde_json::json!({"error": error.to_data_or_id().unwrap_left()}),
			};
		value.to_string()
	}
}

fn to_exception(python: Python<'_>, error: &tg::Error) -> PyErr {
	let result = (|| -> PyResult<_> {
		let data = match error.to_data_or_id() {
			tg::Either::Left(data) => serde_json::to_value(data).unwrap(),
			tg::Either::Right(_) => serde_json::json!(error.to_referent().to_string()),
		};
		let data = python
			.import("json")?
			.call_method1("loads", (data.to_string(),))?;
		python
			.import("tangram")?
			.getattr("Error")?
			.call_method1("from_data", (data,))
	})();
	match result {
		Ok(exception) => PyErr::from_value(exception),
		Err(error) => error,
	}
}

fn serialize_module(module: tg::module::Data) -> tg::Result<String> {
	let resolved = tangram_compiler::python::resolve::Module::new(module);
	let output = serde_json::to_string(&resolved)
		.map_err(|error| tg::error!(!error, "failed to serialize the python module"))?;
	Ok(output)
}

async fn load_module(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
) -> tg::Result<String> {
	let arg = tg::module::load::Arg {
		language: Some(tg::module::load::Language::Python),
		module,
	};
	let output = instance.load_module(arg).await?;
	let output = serde_json::to_string(&output)
		.map_err(|error| tg::error!(!error, "failed to serialize the python module load output"))?;
	Ok(output)
}
