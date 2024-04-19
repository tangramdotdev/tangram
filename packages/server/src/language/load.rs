use super::Server;
use include_dir::include_dir;
use itertools::Itertools as _;
use tangram_client as tg;

const LIB: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Server {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> tg::Result<String> {
		match module {
			// Load a library module.
			tg::Module::Library(module) => {
				let path = module.path.components().iter().skip(1).join("/");
				let text = LIB
					.get_file(&path)
					.ok_or_else(
						|| tg::error!(%path, "could not find a library module at the path"),
					)?
					.contents_utf8()
					.ok_or_else(|| tg::error!("failed to read the file as UTF-8"))?;
				Ok(text.to_owned())
			},

			// Load a module from a document.
			tg::Module::Document(document) => self.get_document_text(document).await,

			// Load a module from a package.
			tg::Module::Normal(module) => {
				// Get the package.
				let package = tg::Directory::with_id(module.package.clone());

				// Load the module.
				let entry = package.get(&self.server, &module.path).await?;
				let file = entry
					.try_unwrap_file_ref()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let text = file.text(&self.server).await?;

				Ok(text)
			},
		}
	}
}
