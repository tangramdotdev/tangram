use {
	crate::syscall::register_syscalls,
	futures::future::BoxFuture,
	include_dir::{Dir, include_dir},
	pyo3::prelude::*,
	std::{path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
};

/// The tangram client package source files.
static TANGRAM_CLIENT: Dir = include_dir!("$CARGO_MANIFEST_DIR/../clients/python/tangram");

/// The runtime Python sources.
const HANDLE_SOURCE: &str = include_str!("handle.py");
const MAIN_SOURCE: &str = include_str!("main.py");
const START_SOURCE: &str = include_str!("start.py");

mod error;
mod module;
mod syscall;

pub type Logger = Arc<
	dyn Fn(tg::process::log::Stream, String) -> BoxFuture<'static, tg::Result<()>>
		+ Send
		+ Sync
		+ 'static,
>;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub output: Option<tg::Value>,
}

pub(crate) struct State {
	pub logger: Logger,
	pub main_runtime_handle: tokio::runtime::Handle,
	pub modules: std::sync::Mutex<Vec<Module>>,
	pub root: tg::module::Data,
	pub handle: tg::handle::dynamic::Handle,
}

#[derive(Clone)]
pub(crate) struct StateHandle(pub Arc<State>);

#[derive(Clone, Debug)]
pub(crate) struct Module {
	pub module: tg::module::Data,
}

pub struct Abort {
	sender: tokio::sync::watch::Sender<bool>,
}

#[expect(clippy::too_many_arguments)]
pub async fn run<H>(
	handle: &H,
	args: tg::value::data::Array,
	cwd: PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
	logger: Logger,
	main_runtime_handle: tokio::runtime::Handle,
	abort_sender: Option<tokio::sync::watch::Sender<Option<Abort>>>,
) -> tg::Result<Output>
where
	H: tg::Handle,
{
	// Convert the executable to a module.
	let module = match &executable {
		tg::command::data::Executable::Artifact(_) => {
			return Err(tg::error!("invalid executable"));
		},

		tg::command::data::Executable::Module(executable) => executable.module.clone(),

		tg::command::data::Executable::Path(executable) => {
			let kind = executable
				.path
				.extension()
				.and_then(|ext| ext.to_str())
				.and_then(|ext| match ext {
					"py" => Some(tg::module::Kind::Py),
					"pyi" => Some(tg::module::Kind::Pyi),
					_ => None,
				})
				.ok_or_else(|| tg::error!("invalid executable"))?;
			let item = tg::module::data::Item::Path(executable.path.clone());
			let options = tg::referent::Options {
				path: Some(executable.path.clone()),
				..Default::default()
			};
			tg::module::Data {
				kind,
				referent: tg::Referent { item, options },
			}
		},
	};

	// Create the abort channel.
	let (abort_channel_sender, _abort_channel_receiver) = tokio::sync::watch::channel(false);

	// Send the abort handle if requested.
	if let Some(abort_sender) = abort_sender {
		let abort = Abort {
			sender: abort_channel_sender,
		};
		abort_sender.send_replace(Some(abort));
	}

	// Create the state.
	let state = Arc::new(State {
		logger,
		main_runtime_handle,
		modules: std::sync::Mutex::new(Vec::new()),
		root: module.clone(),
		handle: tg::handle::dynamic::Handle::new(handle.clone()),
	});

	// Run Python.
	let result = run_python(state, args, cwd, env, executable).await;

	let output = match result {
		Ok(data) => Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(tg::Value::try_from(data)?),
		},
		Err(error) => Output {
			checksum: None,
			error: Some(error),
			exit: 1,
			output: None,
		},
	};

	Ok(output)
}

async fn run_python(
	state: Arc<State>,
	args: tg::value::data::Array,
	cwd: PathBuf,
	env: tg::value::data::Map,
	executable: tg::command::data::Executable,
) -> tg::Result<tg::value::Data> {
	// Initialize Python, set up modules, and call start().
	let result = Python::with_gil(|py| {
		// Create the state handle for Python access.
		let state_handle = StateHandle(state.clone());

		// Install the custom import hook.
		module::install_import_hook(py, state_handle.clone()).map_err(|e| tg::error!("{e}"))?;

		// Register syscalls as a Python module.
		register_syscalls(py, state_handle.clone()).map_err(|e| tg::error!("{e}"))?;

		// Install the tangram client package.
		install_tangram_client(py).map_err(|e| tg::error!("{e}"))?;

		// Install the runtime modules (handle, main, start).
		install_runtime_modules(py).map_err(|e| tg::error!("{e}"))?;

		// Create the argument dictionary.
		let arg_dict = pyo3::types::PyDict::new(py);
		let args_py = pythonize::pythonize(py, &args).map_err(|e| tg::error!("{e}"))?;
		arg_dict
			.set_item("args", args_py)
			.map_err(|e| tg::error!("{e}"))?;
		arg_dict
			.set_item(
				"cwd",
				cwd.to_str().ok_or_else(|| tg::error!("invalid cwd"))?,
			)
			.map_err(|e| tg::error!("{e}"))?;
		let env_py = pythonize::pythonize(py, &env).map_err(|e| tg::error!("{e}"))?;
		arg_dict
			.set_item("env", env_py)
			.map_err(|e| tg::error!("{e}"))?;
		let executable_py = pythonize::pythonize(py, &executable).map_err(|e| tg::error!("{e}"))?;
		arg_dict
			.set_item("executable", executable_py)
			.map_err(|e| tg::error!("{e}"))?;

		// Import the root module.
		let root_module =
			module::import_root_module(py, &state_handle).map_err(|e| tg::error!("{e}"))?;

		// Get the start function from main module.
		let main_module = py.import("__main__").map_err(|e| tg::error!("{e}"))?;
		let start_fn = main_module
			.getattr("start")
			.map_err(|e| tg::error!("failed to get start function: {e}"))?;

		// Call start(arg, root_module).
		let result = start_fn
			.call1((arg_dict, root_module))
			.map_err(|e| tg::error!("failed to call start function: {e}"))?;

		Ok::<_, tg::Error>(result.unbind())
	})?;

	// The start function returns a coroutine, run it with asyncio.run().
	let final_result = Python::with_gil(|py| {
		let result = result.into_bound(py);
		let asyncio = py.import("asyncio")?;
		Ok::<_, pyo3::PyErr>(asyncio.call_method1("run", (result,))?.unbind())
	})
	.map_err(|e| tg::error!("failed to run start coroutine: {e}"))?;

	// Convert the result back to Rust.
	Python::with_gil(|py| {
		let final_result = final_result.bind(py);
		let data: tg::value::Data = pythonize::depythonize(final_result)
			.map_err(|e| tg::error!("failed to convert result: {e}"))?;
		Ok(data)
	})
}

impl Abort {
	pub fn abort(&self) {
		let _ = self.sender.send(true);
	}
}

impl std::ops::Deref for StateHandle {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

/// Install the tangram client package into sys.modules.
fn install_tangram_client(py: Python<'_>) -> pyo3::PyResult<()> {
	let builtins = py.import("builtins")?;
	let compile = builtins.getattr("compile")?;
	let exec = builtins.getattr("exec")?;
	let sys = py.import("sys")?;
	let modules = sys.getattr("modules")?;

	// Create the tangram package module.
	let tangram_module = pyo3::types::PyModule::new(py, "tangram")?;
	tangram_module.setattr("__file__", "<tangram>")?;
	tangram_module.setattr("__package__", "tangram")?;
	tangram_module.setattr("__path__", pyo3::types::PyList::empty(py))?;

	// Add tangram to sys.modules FIRST.
	modules.set_item("tangram", &tangram_module)?;

	// Collect all submodule info.
	let mut submodule_sources: std::collections::HashMap<String, &str> =
		std::collections::HashMap::new();
	for entry in TANGRAM_CLIENT.files() {
		let path = entry.path();
		let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
		let stem = path.file_stem().and_then(|n| n.to_str()).unwrap_or("");

		// Skip non-.py files.
		if !std::path::Path::new(file_name)
			.extension()
			.is_some_and(|ext| ext.eq_ignore_ascii_case("py"))
		{
			continue;
		}

		let source = entry
			.contents_utf8()
			.ok_or_else(|| pyo3::exceptions::PyValueError::new_err("invalid UTF-8"))?;

		submodule_sources.insert(stem.to_string(), source);
	}

	// Execute submodules in dependency order.
	// The order is determined by the import structure of the modules.
	// Base modules (no tangram.* deps): util, handle, object_, value, placeholder, mutation
	// Then modules that depend on those: blob, symlink, template, file, directory
	// Then higher-level modules: command, process, build, run, resolve
	// Then utility modules: assert_, encoding, path, builtin
	// Finally __init__.py
	let execution_order = [
		"util",
		"handle",
		"object_",
		"value",
		"placeholder",
		"mutation",
		"resolve",
		"args",
		// New simple type modules.
		"reference",
		"tag",
		"position",
		"range_",
		"referent",
		// Blob must come before modules that depend on it.
		"blob",
		"symlink",
		"template",
		"file",
		"directory",
		// New type modules that depend on artifacts.
		"artifact",
		"module_",
		"location",
		"diagnostic",
		"checksum",
		"error_",
		"graph",
		// Command and higher-level modules.
		"command",
		"process",
		"build",
		"run",
		"assert_",
		"encoding",
		"path",
		"builtin",
	];

	for stem in execution_order {
		if let Some(source) = submodule_sources.get(stem) {
			let submodule_name = format!("tangram.{stem}");
			let submodule = pyo3::types::PyModule::new(py, &submodule_name)?;
			submodule.setattr("__file__", format!("<tangram/{stem}.py>"))?;
			submodule.setattr("__package__", "tangram")?;

			// Add to sys.modules BEFORE execution so circular imports work.
			modules.set_item(&submodule_name, &submodule)?;

			// Execute the source.
			let compiled = compile.call1((*source, format!("tangram/{stem}.py"), "exec"))?;
			exec.call1((compiled, submodule.dict()))?;
		}
	}

	// Now execute __init__.py in the main tangram module.
	if let Some(source) = submodule_sources.get("__init__") {
		let compiled = compile.call1((*source, "tangram/__init__.py", "exec"))?;
		exec.call1((compiled, tangram_module.dict()))?;
	}

	// Also add as tg alias for compatibility.
	modules.set_item("tg", &tangram_module)?;

	// Add tg to builtins for ambient access.
	builtins.setattr("tg", &tangram_module)?;

	Ok(())
}

/// Install the runtime modules (handle, main, start).
fn install_runtime_modules(py: Python<'_>) -> pyo3::PyResult<()> {
	let builtins = py.import("builtins")?;
	let compile = builtins.getattr("compile")?;
	let exec = builtins.getattr("exec")?;
	let sys = py.import("sys")?;
	let modules = sys.getattr("modules")?;

	// Install handle module.
	let handle_module = pyo3::types::PyModule::new(py, "handle")?;
	handle_module.setattr("__file__", "<handle>")?;
	let compiled = compile.call1((HANDLE_SOURCE, "handle.py", "exec"))?;
	exec.call1((compiled, handle_module.dict()))?;
	modules.set_item("handle", &handle_module)?;

	// Install start module.
	let start_module = pyo3::types::PyModule::new(py, "start")?;
	start_module.setattr("__file__", "<start>")?;
	let compiled = compile.call1((START_SOURCE, "start.py", "exec"))?;
	exec.call1((compiled, start_module.dict()))?;
	modules.set_item("start", &start_module)?;

	// Execute main.py in __main__ to set up tg and start.
	let main_module = py.import("__main__")?;
	let compiled = compile.call1((MAIN_SOURCE, "main.py", "exec"))?;
	exec.call1((compiled, main_module.dict()))?;

	Ok(())
}
