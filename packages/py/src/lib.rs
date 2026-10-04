use {
	pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyModule},
	std::{
		ffi::CString,
		fmt::Write as _,
		path::{Path, PathBuf},
		sync::{Mutex, mpsc},
	},
	tangram_client::prelude::*,
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

#[derive(serde::Serialize)]
struct Resolved {
	data: tg::module::Data,
	filename: PathBuf,
	key: String,
}

pub fn run(arg: Arg) -> tg::Result<Outcome> {
	// Serialize invocations because CPython modules and process context are shared.
	let _guard = RUN
		.lock()
		.map_err(|_| tg::error!("the Python runtime lock is poisoned"))?;

	// Preserve the entry's graph pointer before creating the process context and module cache.
	let module = arg
		.main_runtime_handle
		.block_on(prepare_module(&arg.instance, arg.module))
		.map_err(|error| tg::error!(!error, "failed to prepare the Python entry module"))?;

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
	.map_err(|error| tg::error!(!error, "failed to execute the Python runtime"))?;

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

	fn resolve(&self, py: Python<'_>, referrer: &str, import: &str) -> PyResult<String> {
		let referrer = serde_json::from_str::<tg::module::Data>(referrer)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let import = serde_json::from_str::<tg::module::Import>(import)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let arg = tg::module::resolve::Arg {
			import,
			referrer: Some(referrer),
		};
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let result = async {
					let module = instance.resolve_module(arg).await?.module;
					serialize_module(module)
				}
				.await;
				let _ = sender.send(result);
			}
		});
		let result = py
			.detach(move || receiver.recv())
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		result.map_err(|error| to_exception(py, &error))
	}

	fn resolve_path(
		&self,
		py: Python<'_>,
		referrer: &str,
		path: PathBuf,
		kind: &str,
	) -> PyResult<Option<String>> {
		let referrer = serde_json::from_str::<tg::module::Data>(referrer)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let kind = kind
			.parse::<tg::module::Kind>()
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let result = resolve_path(instance, referrer, path, kind).await;
				let _ = sender.send(result);
			}
		});
		let result = py
			.detach(move || receiver.recv())
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		result.map_err(|error| to_exception(py, &error))
	}

	fn namespace_exists(&self, py: Python<'_>, referrer: &str, path: PathBuf) -> PyResult<bool> {
		let module = serde_json::from_str::<tg::module::Data>(referrer)
			.map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
		let (sender, receiver) = mpsc::channel();
		self.main_runtime_handle.spawn({
			let instance = self.instance.clone();
			async move {
				let result = namespace_exists(instance, module, path).await;
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

async fn prepare_module(
	instance: &tg::instance::dynamic::Instance,
	mut module: tg::module::Data,
) -> tg::Result<tg::module::Data> {
	if module.kind == tg::module::Kind::Py
		&& matches!(
			module.referent.node,
			tg::module::data::Source::Edge(tg::graph::data::Edge::Object(_))
		) {
		let file = module_file(&module)?;
		if let tg::file::Object::Pointer(pointer) =
			file.object_with_instance(instance).await?.as_ref()
		{
			module.referent.node =
				tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer.to_data()));
			module
				.referent
				.options
				.tokens
				.inherit(&pointer.graph.state().tokens());
		}
	}
	Ok(module)
}

fn serialize_module(module: tg::module::Data) -> tg::Result<String> {
	let filename = match &module.referent.node {
		tg::module::data::Source::Path(path) => path.clone(),
		tg::module::data::Source::Edge(_) => module
			.referent
			.path()
			.unwrap_or_else(|| Path::new("tangram.py"))
			.to_owned(),
	};
	let key = module.without_token().to_string();
	let resolved = Resolved {
		data: module,
		filename,
		key,
	};
	let output = serde_json::to_string(&resolved)
		.map_err(|error| tg::error!(!error, "failed to serialize the Python module"))?;
	Ok(output)
}

async fn load_module(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
) -> tg::Result<String> {
	let arg = tg::module::load::Arg {
		module: module.clone(),
	};
	let text = match module.kind {
		tg::module::Kind::Py => instance.load_module(arg).await?.text,
		tg::module::Kind::Js | tg::module::Kind::Ts | tg::module::Kind::Dts => {
			return Err(tg::error!(
				"cannot execute a {} module in Python",
				module.kind
			));
		},
		_ => object_module(&module)?,
	};
	Ok(text)
}

async fn resolve_path(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
	path: PathBuf,
	kind: tg::module::Kind,
) -> tg::Result<Option<String>> {
	// Translate a Python path to an import before calling the shared resolver.
	let (referrer, reference) = match &module.referent.node {
		tg::module::data::Source::Path(source) => {
			let source = source.parent().unwrap().join(&path);
			let metadata = match tokio::fs::metadata(&source).await {
				Ok(metadata) => metadata,
				Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
				Err(error) => {
					return Err(tg::error!(!error, "failed to inspect the Python import"));
				},
			};
			if (kind == tg::module::Kind::Directory && !metadata.is_dir())
				|| (kind == tg::module::Kind::Py && !metadata.is_file())
			{
				return Ok(None);
			}
			(Some(module.clone()), tg::Reference::with_path(path.clone()))
		},
		tg::module::data::Source::Edge(_) => {
			let file = module_file(&module)?;
			let dependencies = file.dependencies_with_instance(&instance).await?;
			let dependency = dependencies.into_iter().find(|(reference, _)| {
				let Ok(relative) = reference.node().try_unwrap_path_ref() else {
					return false;
				};
				reference.without_token().options() == &tg::reference::Options::default()
					&& tangram_util::path::normalize(relative)
						== tangram_util::path::normalize(&path)
			});
			if let Some((reference, _)) = dependency {
				(Some(module.clone()), reference)
			} else {
				let Some(member) =
					package_member(instance.clone(), module, path.clone(), kind).await?
				else {
					return Ok(None);
				};
				let reference = member.to_string().parse().map_err(|error| {
					tg::error!(!error, "failed to parse the Python member import")
				})?;
				(None, reference)
			}
		},
	};
	let import = tg::module::Import {
		kind: Some(kind),
		reference,
	};
	let arg = tg::module::resolve::Arg { import, referrer };
	let module = instance.resolve_module(arg).await?.module;
	let output = serialize_module(module)?;
	Ok(Some(output))
}

async fn namespace_exists(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
	path: PathBuf,
) -> tg::Result<bool> {
	if !matches!(module.referent.node, tg::module::data::Source::Edge(_)) {
		return Ok(false);
	}
	// A file-only checkin can retain a namespace's members without a directory object.
	let path = tangram_util::path::normalize(&path);
	let file = module_file(&module)?;
	let dependencies = file.dependencies_with_instance(&instance).await?;
	let exists = dependencies.into_iter().any(|(reference, dependency)| {
		if reference.without_token().options() != &tg::reference::Options::default()
			|| dependency.is_none()
		{
			return false;
		}
		let Ok(relative) = reference.node().try_unwrap_path_ref() else {
			return false;
		};
		let relative = tangram_util::path::normalize(relative);
		relative != path && relative.starts_with(&path)
	});

	Ok(exists)
}

fn module_file(module: &tg::module::Data) -> tg::Result<tg::File> {
	let source = tg::Module::try_from_data(module.clone())?.referent.node;
	let tg::module::Source::Edge(edge) = source else {
		return Err(tg::error!("expected a checked-in Python module"));
	};
	let file = match edge {
		tg::graph::Edge::Object(object) => object
			.try_unwrap_file()
			.map_err(|_| tg::error!("expected a Python module file"))?,
		tg::graph::Edge::Pointer(pointer) => {
			pointer
				.graph
				.state()
				.set_location(module.referent.options.location.clone());
			pointer
				.graph
				.state()
				.set_tokens(module.referent.options.tokens.clone());
			tg::Artifact::with_pointer(pointer)
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a Python module file"))?
		},
		tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
	};
	file.state()
		.set_tokens(module.referent.options.tokens.clone());
	file.state()
		.set_location(module.referent.options.location.clone());
	Ok(file)
}

async fn package_member(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
	path: PathBuf,
	kind: tg::module::Kind,
) -> tg::Result<Option<tg::Referent<tg::module::data::Source>>> {
	// Locate package members within the resolved directory artifact.
	let Some(id) = module.referent.options.id.clone() else {
		return Ok(None);
	};
	let referent = tg::Referent::new(id, module.referent.options.clone());
	let Ok(directory) = tg::Object::with_referent(referent).try_unwrap_directory() else {
		return Ok(None);
	};
	let Some(parent) = module.referent.path().and_then(Path::parent) else {
		return Ok(None);
	};
	let path = tangram_util::path::normalize(parent.join(path));
	if path.is_absolute() || path.starts_with("..") {
		return Ok(None);
	}
	if path.as_os_str().is_empty() || path == Path::new(".") {
		if kind != tg::module::Kind::Directory {
			return Ok(None);
		}
		let mut options = module.referent.options;
		options.path = Some(path);
		let node =
			tg::module::data::Source::Edge(tg::graph::data::Edge::Object(directory.id().into()));
		let referent = tg::Referent { node, options };
		return Ok(Some(referent));
	}
	let mut current = directory;
	let mut components = path.components().peekable();
	let mut edge = None;
	while let Some(component) = components.next() {
		let name = component
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("invalid package member path"))?;
		let Some(entry) = current
			.try_get_entry_edge_with_instance(&instance, name)
			.await?
		else {
			return Ok(None);
		};
		if components.peek().is_some() {
			let artifact = tg::Artifact::with_edge(entry)?;
			let Ok(directory) = artifact.try_unwrap_directory() else {
				return Ok(None);
			};
			current = directory;
		} else {
			edge = Some(entry);
		}
	}
	let Some(edge) = edge else {
		return Ok(None);
	};
	let artifact = tg::Artifact::with_edge(edge.clone())?;
	if !matches!(
		(kind, artifact),
		(tg::module::Kind::Directory, tg::Artifact::Directory(_))
			| (tg::module::Kind::Py, tg::Artifact::File(_))
	) {
		return Ok(None);
	}
	let edge = match edge {
		tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
		tg::graph::Edge::Object(artifact) => tg::graph::Edge::Object(tg::Object::from(artifact)),
		tg::graph::Edge::Pointer(pointer) => tg::graph::Edge::Pointer(pointer),
	};
	let mut options = module.referent.options;
	options.path = Some(path);
	let referent = tg::Referent {
		node: tg::module::data::Source::Edge(edge.to_data()),
		options,
	};
	Ok(Some(referent))
}

fn object_module(module: &tg::module::Data) -> tg::Result<String> {
	let class = match module.kind {
		tg::module::Kind::Artifact => "Artifact",
		tg::module::Kind::Blob => "Blob",
		tg::module::Kind::Command => "Command",
		tg::module::Kind::Directory => "Directory",
		tg::module::Kind::Error => "Error",
		tg::module::Kind::File => "File",
		tg::module::Kind::Graph => "Graph",
		tg::module::Kind::Object => "Object",
		tg::module::Kind::Symlink => "Symlink",
		tg::module::Kind::Dts
		| tg::module::Kind::Js
		| tg::module::Kind::Py
		| tg::module::Kind::Ts => return Err(tg::error!("expected an object module")),
	};
	let mut prefix = String::new();
	let expression = match &module.referent.node {
		tg::module::data::Source::Edge(edge) => match edge {
			tg::graph::data::Edge::Index(_) => return Err(tg::error!("missing graph")),
			tg::graph::data::Edge::Object(id) => {
				let id = serde_json::to_string(&id.to_string()).unwrap();
				format!("tg.{class}.with_id({id})")
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				let pointer = serde_json::to_string(&pointer.to_string()).unwrap();
				prefix = format!("pointer = tg.Graph.Pointer.from_data_string({pointer})\n");
				let class = if class == "Object" { "Artifact" } else { class };
				format!("tg.{class}.with_pointer(pointer)")
			},
		},
		tg::module::data::Source::Path(_) => "None".to_owned(),
	};
	let tokens = serde_json::to_string(&module.referent.options.tokens).unwrap();
	let tokens = serde_json::to_string(&tokens).unwrap();
	let mut text = format!("{prefix}default = {expression}\n");
	if !matches!(module.referent.node, tg::module::data::Source::Path(_)) {
		writeln!(
			text,
			"tg.Object.inherit_tokens(default, __import__('json').loads({tokens}))"
		)
		.unwrap();
		text.push_str(
			"tg.Object.inherit_location(default, __tangram_module__.referent.options.get(\"location\"))\n",
		);
		if !prefix.is_empty() {
			text.push_str("tg.Object.inherit_location(pointer.graph, default.state.location)\n");
			text.push_str("tg.Object.inherit_tokens(pointer.graph, default.state.tokens)\n");
		}
	}
	Ok(text)
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
			let file = module_file(&module).unwrap();
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
