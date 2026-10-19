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
