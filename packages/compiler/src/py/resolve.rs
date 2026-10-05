use {
	std::{
		collections::{BTreeMap, BTreeSet},
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

/// The Python import policy shared by the runtime and the type checker.
pub struct Resolver {
	instance: tg::instance::dynamic::Instance,
	paths: BTreeSet<PathBuf>,
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
	Child {
		context: Context,
		name: String,
	},
	Import {
		imports: BTreeMap<String, tg::module::Import>,
		level: u32,
		name: String,
		referrer: tg::module::Data,
	},
	Member {
		context: Context,
		export: Export,
		name: String,
	},
	Package {
		module: tg::module::Data,
		parent: bool,
	},
	Relative {
		level: u32,
		name: String,
		package: Target,
	},
}

#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Export {
	Absent,
	Module,
	Value,
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Output {
	Fallback,
	Missing {
		message: String,
		name: Option<String>,
	},
	Resolved(Resolution),
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Resolution {
	pub context: Option<Context>,
	pub root: Option<Target>,
	pub steps: Vec<Step>,
	pub target: Target,
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Context {
	pub parent: Target,
	pub prefix: PathBuf,
	pub referrer: tg::module::Data,
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Step {
	pub name: String,
	pub parent: Target,
	pub target: Target,
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Target {
	Module(Module),
	Namespace(Box<Namespace>),
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Module {
	pub data: tg::module::Data,
	pub filename: PathBuf,
	pub key: String,
	pub package: bool,
}

#[derive(Clone, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Namespace {
	pub key: String,
	pub parent: Target,
	pub prefix: PathBuf,
	pub referrer: tg::module::Data,
}

pub fn module_file(module: &tg::module::Data) -> tg::Result<tg::File> {
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

/// Recover package placement from the module's checked-in self-reference.
pub async fn prepare_module(
	instance: &tg::instance::dynamic::Instance,
	mut module: tg::module::Data,
) -> tg::Result<tg::module::Data> {
	if module.kind != tg::module::Kind::Py
		|| matches!(module.referent.node, tg::module::data::Source::Path(_))
	{
		return Ok(module);
	}
	let file = module_file(&module)?;
	if let tg::file::Object::Pointer(pointer) = file.object_with_instance(instance).await?.as_ref()
	{
		module.referent.node =
			tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer.to_data()));
		module
			.referent
			.options
			.tokens
			.inherit(&pointer.graph.state().tokens());
	}
	let dependencies = file.dependencies_with_instance(instance).await?;
	let mut paths = Vec::new();
	for (reference, dependency) in dependencies {
		if reference.without_token().options() != &tg::reference::Options::default() {
			continue;
		}
		let Some(object) = dependency.and_then(|dependency| dependency.0.node) else {
			continue;
		};
		if object.id() != tg::object::Id::from(file.id()) {
			continue;
		}
		let Ok(path) = reference.node().try_unwrap_path_ref() else {
			continue;
		};
		if path.is_absolute() {
			continue;
		}
		let path: PathBuf = path
			.components()
			.skip_while(|component| {
				matches!(
					component,
					std::path::Component::CurDir | std::path::Component::ParentDir
				)
			})
			.collect();
		paths.push(path);
	}
	if let Some(path) = paths
		.into_iter()
		.max_by_key(|path| path.components().count())
	{
		module.referent.options.id = None;
		module.referent.options.name = None;
		module.referent.options.path = Some(path);
		module.referent.options.tag = None;
	}
	Ok(module)
}

impl Resolver {
	#[must_use]
	pub fn new(instance: tg::instance::dynamic::Instance) -> Self {
		Self {
			instance,
			paths: BTreeSet::new(),
		}
	}

	/// Include unsaved editor files in the filesystem view used by relative imports.
	#[must_use]
	pub(crate) fn with_paths(
		instance: tg::instance::dynamic::Instance,
		paths: BTreeSet<PathBuf>,
	) -> Self {
		Self { instance, paths }
	}

	pub async fn resolve(&self, request: Request) -> tg::Result<Output> {
		match request {
			Request::Child { context, name } => self.walk(context, &name).await,
			Request::Import {
				imports,
				level,
				name,
				referrer,
			} => self.import(referrer, &imports, &name, level).await,
			Request::Member {
				context,
				export,
				name,
			} => {
				if matches!(export, Export::Value) {
					return Ok(Output::Fallback);
				}
				let output = self.walk(context, &name).await?;
				// An absent child preserves an existing export, but a failed recorded edge does not.
				if matches!(export, Export::Module)
					&& matches!(output, Output::Missing { name: Some(_), .. })
				{
					return Ok(Output::Fallback);
				}
				Ok(output)
			},
			Request::Package { module, parent } => {
				let prefix = if parent { ".." } else { "." };
				let package = self.containing(&module, PathBuf::from(prefix)).await?;
				Ok(package.map_or(Output::Fallback, |target| {
					Output::Resolved(Resolution::new(target))
				}))
			},
			Request::Relative {
				level,
				name,
				package,
			} => {
				let Some(context) = package.context() else {
					return Ok(missing(
						"attempted relative import with no known parent package",
						None,
					));
				};
				self.relative(context, &name, level).await
			},
		}
	}

	async fn import(
		&self,
		referrer: tg::module::Data,
		imports: &BTreeMap<String, tg::module::Import>,
		name: &str,
		level: u32,
	) -> tg::Result<Output> {
		let referrer = prepare_module(&self.instance, referrer).await?;
		if level > 0 {
			let Some(parent) = self.containing(&referrer, PathBuf::from(".")).await? else {
				return Ok(missing(
					"attempted relative import with no known parent package",
					None,
				));
			};
			let context = Context {
				parent,
				prefix: PathBuf::from("."),
				referrer,
			};
			return self.relative(context, name, level).await;
		}
		let (root, suffix) = name.split_once('.').unwrap_or((name, ""));
		let Some(import) = imports.get(root) else {
			return Ok(Output::Fallback);
		};
		let arg = tg::module::resolve::Arg {
			import: import.clone(),
			referrer: Some(referrer.clone()),
		};
		let module = self.instance.resolve_module(arg).await?.module;
		// Preserve directory lookup only when the import itself resolves to a directory.
		let directory = if module.kind == tg::module::Kind::Py
			&& matches!(module.referent.node, tg::module::data::Source::Edge(_))
		{
			let import = tg::module::Import {
				kind: Some(tg::module::Kind::Object),
				reference: import.reference.clone(),
			};
			let arg = tg::module::resolve::Arg {
				import,
				referrer: Some(referrer),
			};
			let mut module = self.instance.resolve_module(arg).await?.module;
			let is_directory = match &module.referent.node {
				tg::module::data::Source::Edge(tg::graph::data::Edge::Object(id)) => {
					id.kind() == tg::object::Kind::Directory
				},
				tg::module::data::Source::Edge(tg::graph::data::Edge::Pointer(pointer)) => {
					pointer.kind == tg::artifact::Kind::Directory
				},
				_ => false,
			};
			module.kind = tg::module::Kind::Directory;
			is_directory.then_some(module)
		} else {
			None
		};
		let module = prepare_module(&self.instance, module).await?;
		let target = Target::Module(Module::new(module));
		let context = target.context().map(|mut context| {
			if let Some(directory) = directory {
				context.referrer = directory;
			}
			context
		});
		if suffix.is_empty() {
			let mut resolution = Resolution::new(target);
			resolution.context = context;
			return Ok(Output::Resolved(resolution));
		}
		let Some(context) = context else {
			return Ok(missing(format!("{root:?} is not a package"), Some(name)));
		};
		let mut output = self.walk(context, suffix).await?;
		if let Output::Resolved(resolution) = &mut output {
			resolution.root = Some(target);
		}
		Ok(output)
	}

	async fn relative(&self, mut context: Context, name: &str, level: u32) -> tg::Result<Output> {
		for _ in 1..level {
			let Some(parent) = self.parent(&context.parent).await? else {
				return Ok(missing(
					"attempted relative import beyond top-level package",
					None,
				));
			};
			context.prefix.push("..");
			// Leaving an explicitly imported directory follows the recorded parent package.
			if context.referrer.kind == tg::module::Kind::Directory
				&& tangram_util::path::normalize(&context.prefix).starts_with("..")
			{
				context = parent.context().unwrap();
			} else {
				context.parent = parent;
			}
		}
		self.walk(context, name).await
	}

	async fn walk(&self, mut context: Context, name: &str) -> tg::Result<Output> {
		if name.is_empty() {
			let mut resolution = Resolution::new(context.parent.clone());
			resolution.context = Some(context);
			return Ok(Output::Resolved(resolution));
		}
		let mut root = None;
		let mut steps = Vec::new();
		let mut parts = name.split('.').peekable();
		while let Some(part) = parts.next() {
			context.prefix.push(part);
			let target = match self.child(&context).await? {
				Output::Resolved(resolution) => resolution.target,
				Output::Missing { message, name } => return Ok(Output::Missing { message, name }),
				Output::Fallback => {
					return Ok(missing(
						format!("no Tangram module named {name:?}"),
						Some(name),
					));
				},
			};
			let step = Step {
				name: part.to_owned(),
				parent: context.parent,
				target: target.clone(),
			};
			steps.push(step);
			context.parent = target.clone();
			if let Target::Namespace(_) = &target {
				context = target.context().unwrap();
			}
			if parts.peek().is_none() {
				let context = target.is_package().then_some(context);
				let resolution = Resolution {
					context,
					root,
					steps,
					target,
				};
				return Ok(Output::Resolved(resolution));
			}
			if !target.is_package() {
				return Ok(missing(format!("{part:?} is not a package"), Some(name)));
			}
			root.get_or_insert(target);
		}
		unreachable!()
	}

	async fn child(&self, context: &Context) -> tg::Result<Output> {
		let path = &context.prefix;
		let file = self
			.path(
				&context.referrer,
				PathBuf::from(format!("{}.tg.py", path.display())),
				tg::module::Kind::Py,
			)
			.await?;
		let initializer = self
			.path(
				&context.referrer,
				path.join("tangram.py"),
				tg::module::Kind::Py,
			)
			.await?;
		let module = match (file, initializer) {
			(Some(_), Some(_)) => {
				return Ok(missing(
					format!(
						"ambiguous Tangram module: {}.tg.py and {}/tangram.py",
						path.display(),
						path.display()
					),
					None,
				));
			},
			(Some(module), None) | (None, Some(module)) => Some(module),
			(None, None) => None,
		};
		if let Some(module) = module {
			let target = Target::Module(Module::new(module));
			return Ok(Output::Resolved(Resolution::new(target)));
		}
		let directory = self
			.path(&context.referrer, path.clone(), tg::module::Kind::Directory)
			.await?;
		if directory.is_none()
			&& !namespace_exists(
				self.instance.clone(),
				context.referrer.clone(),
				path.clone(),
			)
			.await?
		{
			return Ok(Output::Fallback);
		}
		let Some(name) = path.file_name() else {
			return Ok(Output::Fallback);
		};
		let name = name.to_string_lossy();
		let (referrer, prefix) = match directory {
			Some(directory)
				if context.referrer.kind != tg::module::Kind::Directory
					&& matches!(directory.referent.node, tg::module::data::Source::Edge(_)) =>
			{
				(directory, PathBuf::from("."))
			},
			_ => (context.referrer.clone(), path.clone()),
		};
		let target = Self::namespace(referrer, prefix, context.parent.clone(), &name);
		Ok(Output::Resolved(Resolution::new(target)))
	}

	async fn containing(
		&self,
		module: &tg::module::Data,
		mut prefix: PathBuf,
	) -> tg::Result<Option<Target>> {
		let module = prepare_module(&self.instance, module.clone()).await?;
		let module = &module;
		let descriptor = Module::new(module.clone());
		if prefix == Path::new(".") && descriptor.package {
			return Ok(Some(Target::Module(descriptor)));
		}
		let mut namespaces = Vec::new();
		let mut target = loop {
			prefix = tangram_util::path::normalize(&prefix);
			if prefix.as_os_str().is_empty() {
				prefix.push(".");
			}
			let directory = descriptor
				.filename
				.parent()
				.map(|parent| tangram_util::path::normalize(parent.join(&prefix)));
			if matches!(module.referent.node, tg::module::data::Source::Edge(_))
				&& directory
					.as_ref()
					.is_some_and(|path| path.is_absolute() || path.starts_with(".."))
			{
				return Ok(None);
			}
			if let Some(initializer) = self
				.path(module, prefix.join("tangram.py"), tg::module::Kind::Py)
				.await?
			{
				if initializer.has_same_identity(module) {
					return Ok(None);
				}
				break Target::Module(Module::new(initializer));
			}
			let Some(directory) = directory else {
				return Ok(None);
			};
			if directory.as_os_str().is_empty()
				|| directory == Path::new(".")
				|| directory == Path::new("/")
				|| directory.starts_with("..")
			{
				return Ok(None);
			}
			let Some(name) = directory.file_name() else {
				return Ok(None);
			};
			namespaces.push((prefix.clone(), name.to_string_lossy().into_owned()));
			prefix.push("..");
		};
		for (prefix, name) in namespaces.into_iter().rev() {
			target = Self::namespace(module.clone(), prefix, target, &name);
		}
		Ok(Some(target))
	}

	async fn parent(&self, target: &Target) -> tg::Result<Option<Target>> {
		match target {
			Target::Module(module) => self.containing(&module.data, PathBuf::from("..")).await,
			Target::Namespace(namespace) => Ok(Some(namespace.parent.clone())),
		}
	}

	fn namespace(
		referrer: tg::module::Data,
		prefix: PathBuf,
		parent: Target,
		name: &str,
	) -> Target {
		let key = format!("namespace:{}/{name}", parent.key());
		let namespace = Namespace {
			key,
			parent,
			prefix,
			referrer,
		};
		Target::Namespace(Box::new(namespace))
	}

	async fn path(
		&self,
		module: &tg::module::Data,
		path: PathBuf,
		kind: tg::module::Kind,
	) -> tg::Result<Option<tg::module::Data>> {
		if let tg::module::data::Source::Path(source) = &module.referent.node {
			let target = tangram_util::path::normalize(source.parent().unwrap().join(&path));
			let exists = match kind {
				tg::module::Kind::Py => self.paths.contains(&target),
				tg::module::Kind::Directory => self
					.paths
					.iter()
					.any(|path| path != &target && path.starts_with(&target)),
				_ => false,
			};
			if exists {
				let module = tg::module::Data {
					kind,
					referent: tg::Referent::with_node(tg::module::data::Source::Path(target)),
				};
				return Ok(Some(module));
			}
		}
		resolve_path(self.instance.clone(), module.clone(), path, kind).await
	}
}

impl Output {
	#[must_use]
	pub(crate) fn without_token(&self) -> Self {
		let mut output = self.clone();
		if let Self::Resolved(resolution) = &mut output {
			if let Some(context) = &mut resolution.context {
				context.clear_tokens();
			}
			if let Some(root) = &mut resolution.root {
				root.clear_tokens();
			}
			for step in &mut resolution.steps {
				step.parent.clear_tokens();
				step.target.clear_tokens();
			}
			resolution.target.clear_tokens();
		}
		output
	}
}

impl Resolution {
	#[must_use]
	pub fn new(target: Target) -> Self {
		let context = target.context();
		Self {
			context,
			root: None,
			steps: Vec::new(),
			target,
		}
	}
}

impl Context {
	fn clear_tokens(&mut self) {
		self.parent.clear_tokens();
		self.referrer.referent.options.tokens.clear();
	}
}

impl Target {
	#[must_use]
	pub fn key(&self) -> &str {
		match self {
			Self::Module(module) => &module.key,
			Self::Namespace(namespace) => &namespace.key,
		}
	}

	#[must_use]
	pub fn is_package(&self) -> bool {
		match self {
			Self::Module(module) => module.package,
			Self::Namespace(_) => true,
		}
	}

	#[must_use]
	pub fn context(&self) -> Option<Context> {
		let (referrer, prefix) = match self {
			Self::Module(module) if module.package => (module.data.clone(), PathBuf::from(".")),
			Self::Module(_) => return None,
			Self::Namespace(namespace) => (namespace.referrer.clone(), namespace.prefix.clone()),
		};
		Some(Context {
			parent: self.clone(),
			prefix,
			referrer,
		})
	}

	fn clear_tokens(&mut self) {
		match self {
			Self::Module(module) => module.data.referent.options.tokens.clear(),
			Self::Namespace(namespace) => {
				namespace.parent.clear_tokens();
				namespace.referrer.referent.options.tokens.clear();
			},
		}
	}
}

impl Module {
	#[must_use]
	pub fn new(data: tg::module::Data) -> Self {
		let filename = module_path(&data)
			.unwrap_or_else(|| Path::new("tangram.py"))
			.to_owned();
		let key = data.without_token().to_string();
		let package = data.kind == tg::module::Kind::Py
			&& filename
				.file_name()
				.is_some_and(|name| name == "tangram.py");
		Self {
			data,
			filename,
			key,
			package,
		}
	}
}

fn module_path(module: &tg::module::Data) -> Option<&Path> {
	match &module.referent.node {
		tg::module::data::Source::Edge(_) => module.referent.path(),
		tg::module::data::Source::Path(path) => Some(path),
	}
}

fn missing(message: impl Into<String>, name: Option<&str>) -> Output {
	Output::Missing {
		message: message.into(),
		name: name.map(str::to_owned),
	}
}

async fn resolve_path(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
	path: PathBuf,
	kind: tg::module::Kind,
) -> tg::Result<Option<tg::module::Data>> {
	if module.kind == tg::module::Kind::Directory {
		let Some(referent) = directory_member(instance.clone(), module, path, kind).await? else {
			return Ok(None);
		};
		let module = tg::module::Data { kind, referent };
		return Ok(Some(prepare_module(&instance, module).await?));
	}
	// Translate a Python path to an import before calling the shared resolver.
	let reference = match &module.referent.node {
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
			tg::Reference::with_path(path)
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
			let Some((reference, _)) = dependency else {
				return Ok(None);
			};
			reference
		},
	};
	let import = tg::module::Import {
		kind: Some(kind),
		reference,
	};
	let arg = tg::module::resolve::Arg {
		import,
		referrer: Some(module),
	};
	let module = instance.resolve_module(arg).await?.module;
	let module = prepare_module(&instance, module).await?;
	Ok(Some(module))
}

async fn namespace_exists(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
	path: PathBuf,
) -> tg::Result<bool> {
	if module.kind == tg::module::Kind::Directory
		|| !matches!(module.referent.node, tg::module::data::Source::Edge(_))
	{
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

async fn directory_member(
	instance: tg::instance::dynamic::Instance,
	module: tg::module::Data,
	path: PathBuf,
	kind: tg::module::Kind,
) -> tg::Result<Option<tg::Referent<tg::module::data::Source>>> {
	let source = tg::Module::try_from_data(module.clone())?.referent.node;
	let tg::module::Source::Edge(edge) = source else {
		return Ok(None);
	};
	let directory = match edge {
		tg::graph::Edge::Object(object) => object
			.try_unwrap_directory()
			.map_err(|_| tg::error!("expected a directory"))?,
		tg::graph::Edge::Pointer(pointer) => tg::Artifact::with_pointer(pointer)
			.try_unwrap_directory()
			.map_err(|_| tg::error!("expected a directory"))?,
		tg::graph::Edge::Index(_) => return Err(tg::error!("missing graph")),
	};
	directory
		.state()
		.set_location(module.referent.options.location.clone());
	directory
		.state()
		.set_tokens(module.referent.options.tokens.clone());
	let path = tangram_util::path::normalize(path);
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn module_descriptors_use_the_canonical_source_path() {
		for (path, package) in [("/test/main.tg.py", false), ("/test/tangram.py", true)] {
			for node in [
				tg::module::data::Source::Path(PathBuf::from(path)),
				tg::module::data::Source::Edge(tg::graph::data::Edge::Object(
					tg::file::Id::new(b"module").into(),
				)),
			] {
				let options = if matches!(node, tg::module::data::Source::Edge(_)) {
					tg::referent::Options {
						path: Some(PathBuf::from(path)),
						..Default::default()
					}
				} else {
					tg::referent::Options::default()
				};
				let referent = tg::Referent { node, options };
				let data = tg::module::Data {
					kind: tg::module::Kind::Py,
					referent,
				};
				let module = Module::new(data.clone());
				assert_eq!(module.filename, Path::new(path));
				assert_eq!(module.package, package);
				assert_eq!(module.key, data.without_token().to_string());
			}
		}
	}
}
