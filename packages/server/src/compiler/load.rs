use super::Compiler;
use include_dir::include_dir;
use tangram_client as tg;
use tangram_either::Either;

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
				let file_name = path.file_name().ok_or_else(|| tg::error!("invalid path"))?;
				let file = LIBRARY
					.get_file(file_name)
					.ok_or_else(|| tg::error!(?file_name, "failed to find the library module"))?;
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
					path.clone().join(subpath)
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

				let file = match &object {
					tg::Object::Directory(directory) => {
						let root_module = tg::package::try_get_root_module_file_name(
							&self.server,
							Either::Left(&object),
						)
						.await?
						.ok_or_else(|| tg::error!("expected a root module"))?;
						directory
							.get(&self.server, root_module)
							.await
							.map_err(|source| tg::error!(!source, "failed to get root module"))?
							.try_unwrap_file()
							.map_err(|source| tg::error!(!source, "expected a file"))?
					},
					tg::Object::File(file) => file.clone(),
					tg::Object::Symlink(symlink) => symlink
						.try_resolve(&self.server)
						.await?
						.ok_or_else(|| tg::error!("the symlink is dangling"))?
						.try_into()
						.ok()
						.ok_or_else(|| tg::error!("the symlink must point to a file"))?,
					_ => {
						return Err(tg::error!("module object must be an artifact"));
					},
				};
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
					| tg::module::Kind::Target,
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
					tg::module::Kind::Target => "Target",
					_ => unreachable!(),
				};
				match item {
					tg::module::Item::Path(_) => Ok(format!(
						r#"export default undefined as unknown as tg.{class};"#
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
