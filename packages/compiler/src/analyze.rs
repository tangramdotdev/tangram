use {super::Compiler, std::collections::HashSet, tangram_client::prelude::*};

#[cfg(feature = "javascript")]
mod javascript;

#[cfg(feature = "python")]
pub mod python;

#[derive(Clone, Debug)]
pub struct Analysis {
	pub diagnostics: Vec<tg::diagnostic::Data>,
	pub imports: HashSet<tg::module::Import, fnv::FnvBuildHasher>,
}

impl Compiler {
	/// Analyze a module.
	pub fn analyze(module: &tg::module::Data, text: &str) -> tg::Result<Analysis> {
		#[cfg(not(any(feature = "javascript", feature = "python")))]
		let _ = text;
		match module.kind {
			tg::module::Kind::Artifact
			| tg::module::Kind::Blob
			| tg::module::Kind::Command
			| tg::module::Kind::Directory
			| tg::module::Kind::Error
			| tg::module::Kind::File
			| tg::module::Kind::Graph
			| tg::module::Kind::Object
			| tg::module::Kind::Symlink => Err(tg::error!(kind = %module.kind, "cannot analyze the module")),
			tg::module::Kind::Json | tg::module::Kind::Toml | tg::module::Kind::Yaml => {
				// Get the imports of the runtime and the dependencies.
				let format = tg::module::config::Format::for_kind(module.kind).unwrap();
				let config = tg::module::Config::parse(text, format)?;
				let mut imports = std::iter::once(&config.runtime)
					.chain(config.dependencies.values())
					.map(tg::module::config::Dependency::import)
					.collect::<tg::Result<HashSet<_, fnv::FnvBuildHasher>>>()?;

				// Import the package of a root config, like a root module that imports its package as `.`.
				let path = match &module.referent.node {
					tg::module::data::Source::Edge(_) => module.referent.path(),
					tg::module::data::Source::Path(path) => Some(path.as_path()),
				};
				let root = path
					.and_then(|path| path.file_name())
					.and_then(|name| name.to_str())
					.is_some_and(|name| tg::module::ROOT_MODULE_FILE_NAMES.contains(&name));
				if root {
					imports.insert(tg::module::Import {
						kind: Some(tg::module::Kind::Directory),
						reference: ".".parse()?,
					});
				}

				// Import the interface beside a config on disk, so that checking in the config includes it.
				if let tg::module::data::Source::Path(path) = &module.referent.node
					&& let Some(name) = tg::module::config::interface_file_name(path)
					&& path.with_file_name(&name).is_file()
				{
					imports.insert(tg::module::Import {
						kind: Some(tg::module::Kind::Interface),
						reference: format!("./{name}").parse()?,
					});
				}

				let analysis = Analysis {
					diagnostics: Vec::new(),
					imports,
				};
				Ok(analysis)
			},
			tg::module::Kind::Interface => {
				// Get the imports of the interface whose paths are relative to it.
				let interface = tg::interface::parse::parse(text)?;
				let mut imports = HashSet::default();
				for import in interface.imports.values() {
					let tg::module::data::Source::Path(path) = &import.referent.node else {
						continue;
					};
					if path.is_absolute() {
						continue;
					}
					let path = path
						.to_str()
						.ok_or_else(|| tg::error!("invalid import path"))?;
					let reference = if path.starts_with('.') {
						path.parse()?
					} else {
						format!("./{path}").parse()?
					};
					imports.insert(tg::module::Import {
						kind: Some(import.kind),
						reference,
					});
				}
				let analysis = Analysis {
					diagnostics: Vec::new(),
					imports,
				};
				Ok(analysis)
			},
			tg::module::Kind::JavaScript
			| tg::module::Kind::TypeScript
			| tg::module::Kind::TypeScriptDeclaration => {
				#[cfg(feature = "javascript")]
				{
					Ok(javascript::analyze(module, text))
				}
				#[cfg(not(feature = "javascript"))]
				{
					Err(tg::error!("the javascript feature is not enabled"))
				}
			},
			tg::module::Kind::Python => {
				#[cfg(feature = "python")]
				{
					let tg::module::data::Source::Path(path) = &module.referent.node else {
						return Err(tg::error!("expected a python module source path"));
					};
					python::analyze(path, text)
				}
				#[cfg(not(feature = "python"))]
				{
					Err(tg::error!("the python feature is not enabled"))
				}
			},
		}
	}
}
