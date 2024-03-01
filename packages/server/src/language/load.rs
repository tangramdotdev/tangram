use super::{document, Module};
use include_dir::include_dir;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};

const TANGRAM_D_TS: &str = include_str!(concat!(
	env!("CARGO_MANIFEST_DIR"),
	"/src/language/tangram.d.ts"
));
const LIB: include_dir::Dir = include_dir!("$CARGO_MANIFEST_DIR/src/language/lib");

impl Module {
	/// Load the module.
	pub async fn load(
		&self,
		tg: &dyn tg::Handle,
		document_store: Option<&document::Store>,
	) -> Result<String> {
		match self {
			// Load a library module.
			Self::Library(module) => {
				let path = module.path.to_string();
				let text = match path.as_str() {
					"tangram.d.ts" => TANGRAM_D_TS,
					_ => LIB
						.get_file(&path)
						.wrap_err_with(|| {
							format!(r#"Could not find a library module with the path "{path}"."#)
						})?
						.contents_utf8()
						.wrap_err("Failed to read the file as UTF-8.")?,
				};
				Ok(text.to_owned())
			},

			// Load a module from a document.
			Self::Document(document) => document.text(document_store.unwrap()).await,

			// Load a module from a package.
			Self::Normal(module) => {
				// Get the package.
				let package = tg::Directory::with_id(module.package.clone());

				// Load the module.
				let entry = package.get(tg, &module.path).await?;
				let file = entry
					.try_unwrap_file_ref()
					.ok()
					.wrap_err("Expected a file.")?;
				let text = file.text(tg).await?;

				Ok(text)
			},
		}
	}
}
