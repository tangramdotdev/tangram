use super::Compiler;
use either::Either;
use include_dir::include_dir;
use tangram_client as tg;

const LIB: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Compiler {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> tg::Result<String> {
		match module {
			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				package: Either::Left(path),
				..
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
					.map_err(|source| tg::error!(!source, "failed to read the file"))?;

				Ok(text)
			},

			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				package: Either::Right(package),
				..
			} => {
				let package = tg::Package::with_id(package.clone());
				let package = package.object(&self.server).await?;
				let object = package.nodes[package.root]
					.object
					.clone()
					.ok_or_else(|| tg::error!("expected the package to have an object"))?;
				let file = tg::File::try_from(object).map_err(|_| tg::error!("expected a file"))?;
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			tg::Module {
				kind: tg::module::Kind::Dts,
				package: Either::Left(path),
				..
			} => {
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
					tg::module::Kind::Artifact
					| tg::module::Kind::Directory
					| tg::module::Kind::File
					| tg::module::Kind::Symlink,
				package,
				..
			} => {
				let class = match module.kind {
					tg::module::Kind::Artifact => "Artifact",
					tg::module::Kind::Directory => "Directory",
					tg::module::Kind::File => "File",
					tg::module::Kind::Symlink => "Symlink",
					_ => unreachable!(),
				};
				let object = match package {
					Either::Left(_) => String::new(),
					Either::Right(package) => {
						let package = tg::Package::with_id(package.clone());
						let object = package.object(&self.server).await?;
						let object = object.nodes[object.root]
							.object
							.clone()
							.ok_or_else(|| tg::error!("expected the package to have an object"))?;
						object.id(&self.server).await?.to_string()
					},
				};
				Ok(format!(r#"export default tg.{class}.withId("{object}");"#))
			},

			_ => Err(tg::error!(%module, "invalid module")),
		}
	}
}
