use super::Compiler;
use include_dir::include_dir;
use tangram_client as tg;

pub const LIBRARY: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Compiler {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> tg::Result<String> {
		match module {
			// Handle a declaration.
			tg::Module {
				kind: tg::module::Kind::Dts,
				referent: tg::Referent {
					item: tg::module::Item::Path(path),
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
			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				referent:
					tg::Referent {
						item: tg::module::Item::Path(path),
						subpath,
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
				let path = if let Some(subpath) = subpath {
					path.join(subpath)
				} else {
					path.clone()
				};
				let text = tokio::fs::read_to_string(&path).await.map_err(
					|source| tg::error!(!source, %path = path.display(), "failed to read the file"),
				)?;

				Ok(text)
			},

			// Handle a JS or TS module from an object.
			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				referent:
					tg::Referent {
						item: tg::module::Item::Object(object),
						subpath,
						..
					},
				..
			} => {
				let object = tg::Object::with_id(object.clone());
				let object = if let Some(subpath) = subpath {
					let tg::Object::Directory(directory) = object else {
						return Err(tg::error!("expected a directory"));
					};
					directory.get(&self.server, subpath).await?.into()
				} else {
					object
				};
				let file = object
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			// Handle object modules.
			tg::Module {
				kind:
					tg::module::Kind::Object
					| tg::module::Kind::Blob
					| tg::module::Kind::Leaf
					| tg::module::Kind::Branch
					| tg::module::Kind::Artifact
					| tg::module::Kind::Directory
					| tg::module::Kind::File
					| tg::module::Kind::Symlink
					| tg::module::Kind::Graph
					| tg::module::Kind::Command,
				referent: tg::Referent { item, subpath, .. },
				..
			} => {
				let class = match module.kind {
					tg::module::Kind::Object => "Object",
					tg::module::Kind::Blob => "Blob",
					tg::module::Kind::Leaf => "Leaf",
					tg::module::Kind::Branch => "Branch",
					tg::module::Kind::Artifact => "Artifact",
					tg::module::Kind::Directory => "Directory",
					tg::module::Kind::File => "File",
					tg::module::Kind::Symlink => "Symlink",
					tg::module::Kind::Graph => "Graph",
					tg::module::Kind::Command => "Target",
					_ => unreachable!(),
				};
				match item {
					tg::module::Item::Path(_) => Ok(format!(
						r"export default undefined as unknown as tg.{class};"
					)),
					tg::module::Item::Object(object) => {
						let object = tg::Object::with_id(object.clone());
						let object = if let Some(subpath) = subpath {
							let tg::Object::Directory(directory) = object else {
								return Err(tg::error!("expected a directory"));
							};
							directory.get(&self.server, subpath).await?.into()
						} else {
							object
						};
						let object = object.id(&self.server).await?;
						Ok(format!(r#"export default tg.{class}.withId("{object}");"#))
					},
				}
			},

			_ => Err(tg::error!("invalid module")),
		}
	}
}
