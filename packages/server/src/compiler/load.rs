use super::Compiler;
use include_dir::include_dir;
use tangram_client as tg;
use tangram_either::Either;

const LIB: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Compiler {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::module::Reference) -> tg::Result<String> {
		match (module.kind(), module.object(), module.path()) {
			// Handle a declaration.
			(tg::module::Kind::Dts, None, Some(path)) => {
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

			// Handle a JS or TS module from a path.
			(tg::module::Kind::Js | tg::module::Kind::Ts, Some(Either::Right(object)), path) => {
				// If there is an opened document, then return its contents.
				if let Some(document) = self.documents.get(module) {
					if document.open {
						return Ok(document.text.clone().unwrap());
					}
				}

				// Otherwise, load from the path.
				let path = if let Some(path) = path {
					object.clone().join(path.clone())
				} else {
					object.clone()
				};
				let text = tokio::fs::read_to_string(&path)
					.await
					.map_err(|source| tg::error!(!source, %path, "failed to read the file"))?;

				Ok(text)
			},

			// Handle a JS or TS module from an object.
			(tg::module::Kind::Js | tg::module::Kind::Ts, Some(Either::Left(object)), path) => {
				let object = tg::Object::with_id(object.clone());
				let object = if let Some(path) = path {
					let tg::Object::Directory(directory) = object else {
						return Err(tg::error!("expected a directory"));
					};
					directory.get(&self.server, path).await?.into()
				} else {
					object
				};
				let file = match object {
					tg::Object::File(file) => file,
					tg::Object::Symlink(symlink) => symlink
						.resolve(&self.server)
						.await?
						.ok_or_else(|| tg::error!("the symlink is dangling"))?
						.try_into()
						.ok()
						.ok_or_else(|| tg::error!("the symlink must point to a file"))?,
					_ => {
						return Err(tg::error!("module object must be a file or symlink"));
					},
				};
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			// Handle object modules.
			(
				tg::module::Kind::Object
				| tg::module::Kind::Blob
				| tg::module::Kind::Leaf
				| tg::module::Kind::Branch
				| tg::module::Kind::Artifact
				| tg::module::Kind::Directory
				| tg::module::Kind::File
				| tg::module::Kind::Symlink
				| tg::module::Kind::Graph
				| tg::module::Kind::Target,
				Some(object),
				_,
			) => {
				let class = match module.kind() {
					tg::module::Kind::Object => "Object",
					tg::module::Kind::Blob => "Blob",
					tg::module::Kind::Leaf => "Leaf",
					tg::module::Kind::Branch => "Branch",
					tg::module::Kind::Artifact => "Artifact",
					tg::module::Kind::Directory => "Directory",
					tg::module::Kind::File => "File",
					tg::module::Kind::Symlink => "Symlink",
					tg::module::Kind::Graph => "Graph",
					tg::module::Kind::Target => "Target",
					_ => unreachable!(),
				};
				match object {
					Either::Left(object) => {
						let object = object.to_string();
						Ok(format!(r#"export default tg.{class}.withId("{object}");"#))
					},
					Either::Right(_) => Ok(format!(
						r#"export default undefined as unknown as tg.{class};"#
					)),
				}
			},

			_ => Err(tg::error!("invalid module")),
		}
	}
}
