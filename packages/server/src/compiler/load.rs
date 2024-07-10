use super::Compiler;
use include_dir::include_dir;
use tangram_client as tg;

const LIB: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Compiler {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> tg::Result<String> {
		match module {
			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				object: tg::module::Object::Path(path),
			} => {
				// If there is an opened document, then return its contents.
				if let Some(document) = self.documents.get(module) {
					if document.open {
						return Ok(document.text.clone().unwrap());
					}
				}

				// Otherwise, load from the path.
				let text = tokio::fs::read_to_string(&path)
					.await
					.map_err(|source| tg::error!(!source, %path, "failed to read the file"))?;

				Ok(text)
			},

			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				object: tg::module::Object::Object(object),
			} => {
				let package = object
					.clone()
					.try_into()
					.map_err(|source| tg::error!(!source, "module object must be a package"))?;
				let package = tg::Package::with_id(package);
				let package = package.object(&self.server).await?;
				let object = package.nodes[package.root]
					.object
					.clone()
					.ok_or_else(|| tg::error!(%object, "expected the package to have an object"))?;
				let file = tg::File::try_from(object).map_err(|_| tg::error!("expected a file"))?;
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			tg::Module {
				kind: tg::module::Kind::Dts,
				object,
			} => {
				let tg::module::Object::Path(path) = object else {
					return Err(tg::error!("dts module must have a path"));
				};
				let path = path
					.components()
					.get(1)
					.ok_or_else(|| tg::error!("invalid path"))?
					.to_string();
				let file = LIB
					.get_file(&path)
					.ok_or_else(|| tg::error!(%path, "failed to find the library module"))?;
				let text = file.contents_utf8().unwrap().to_owned();
				Ok(text)
			},

			tg::Module {
				kind:
					tg::module::Kind::Object
					| tg::module::Kind::Artifact
					| tg::module::Kind::Blob
					| tg::module::Kind::Leaf
					| tg::module::Kind::Branch
					| tg::module::Kind::Directory
					| tg::module::Kind::File
					| tg::module::Kind::Symlink
					| tg::module::Kind::Package
					| tg::module::Kind::Target,
				object,
			} => {
				let class = match module.kind {
					tg::module::Kind::Object => "Object",
					tg::module::Kind::Artifact => "Artifact",
					tg::module::Kind::Blob => "Blob",
					tg::module::Kind::Leaf => "Leaf",
					tg::module::Kind::Branch => "Branch",
					tg::module::Kind::Directory => "Directory",
					tg::module::Kind::File => "File",
					tg::module::Kind::Symlink => "Symlink",
					tg::module::Kind::Package => "Package",
					tg::module::Kind::Target => "Target",
					_ => unreachable!(),
				};
				let object = match object {
					tg::module::Object::Path(_) => String::new(),
					tg::module::Object::Object(object) => object.to_string(),
				};
				Ok(format!(r#"export default tg.{class}.withId("{object}");"#))
			},
		}
	}
}
