use super::Compiler;
use include_dir::include_dir;
use tangram_client as tg;

pub const LIBRARY: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Compiler {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::module::Data) -> tg::Result<String> {
		match module {
			// Handle a declaration.
			tg::module::Data {
				kind: tg::module::Kind::Dts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						..
					},
			} => {
				let file = LIBRARY
					.get_file(path)
					.ok_or_else(|| tg::error!(?path, "failed to find the library module"))?;
				let text = file.contents_utf8().unwrap().to_owned();
				Ok(text)
			},

			// Handle a JS or TS module from a path.
			tg::module::Data {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Path(path),
						..
					},
				..
			} => {
				// If there is an opened document, then return its contents.
				if let Some(document) = self.documents.get(module) {
					if document.open {
						return Ok(document.text.clone().unwrap());
					}
				}

				// Otherwise, load from the path.
				let text = tokio::fs::read_to_string(&path).await.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to read the file"),
				)?;

				Ok(text)
			},

			// Handle a JS or TS module from an object.
			tg::module::Data {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				referent:
					tg::Referent {
						item: tg::module::data::Item::Object(object),
						..
					},
				..
			} => {
				let object = tg::Object::with_id(object.clone());
				let file = object
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			// Handle object modules.
			tg::module::Data {
				kind:
					tg::module::Kind::Object
					| tg::module::Kind::Blob
					| tg::module::Kind::Artifact
					| tg::module::Kind::Directory
					| tg::module::Kind::File
					| tg::module::Kind::Symlink
					| tg::module::Kind::Graph
					| tg::module::Kind::Command,
				referent: tg::Referent { item, .. },
				..
			} => {
				let class = match module.kind {
					tg::module::Kind::Object => "Object",
					tg::module::Kind::Blob => "Blob",
					tg::module::Kind::Artifact => "Artifact",
					tg::module::Kind::Directory => "Directory",
					tg::module::Kind::File => "File",
					tg::module::Kind::Symlink => "Symlink",
					tg::module::Kind::Graph => "Graph",
					tg::module::Kind::Command => "Command",
					_ => unreachable!(),
				};
				match item {
					tg::module::data::Item::Path(_) => Ok(format!(
						r"export default undefined as unknown as tg.{class};"
					)),
					tg::module::data::Item::Object(object) => {
						Ok(format!(r#"export default tg.{class}.withId("{object}");"#))
					},
				}
			},

			_ => Err(tg::error!("invalid module")),
		}
	}
}
