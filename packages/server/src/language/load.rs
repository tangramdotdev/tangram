use super::Server;
use include_dir::include_dir;
use tangram_client as tg;
use tangram_error::{error, Result};

const TANGRAM_D_TS: &str = include_str!(concat!(
	env!("CARGO_MANIFEST_DIR"),
	"/src/language/tangram.d.ts"
));

const LIB: include_dir::Dir = include_dir!("$CARGO_MANIFEST_DIR/src/language/lib");

impl Server {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> Result<String> {
		match module {
			// Load a library module.
			tg::Module::Library(module) => {
				let path = module.path.to_string();
				let text = match path.as_str() {
					"tangram.d.ts" => TANGRAM_D_TS,
					_ => LIB
						.get_file(&path)
						.ok_or_else(
							|| error!(%path, "could not find a library module at the path"),
						)?
						.contents_utf8()
						.ok_or_else(|| error!("failed to read the file as UTF-8"))?,
				};
				Ok(text.to_owned())
			},

			// Load a module from a document.
			tg::Module::Document(document) => self.get_document_text(document).await,

			// Load a module from a package.
			tg::Module::Normal(module) => {
				// Get the package.
				let package = tg::Directory::with_id(module.package.clone());

				// Load the module.
				let entry = package.get(&self.inner.server, &module.path).await?;
				let file = entry
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| error!("expected a file"))?;
				let text = file.text(&self.inner.server).await?;

				Ok(text)
			},
		}
	}
}
