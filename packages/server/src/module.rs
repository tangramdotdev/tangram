use {crate::Server, std::path::Path, tangram_client as tg};

pub mod analyze;
pub mod diagnostic;
pub mod load;
pub mod resolve;
pub mod transpile;

impl Server {
	#[allow(dead_code)]
	pub(crate) async fn module_for_path(&self, path: &Path) -> tg::Result<tg::module::Data> {
		// Get the file name.
		let file_name = path
			.file_name()
			.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))?
			.to_str()
			.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))?;
		if !tg::package::is_module_path(file_name.as_ref()) {
			return Err(tg::error!(%path = path.display(), "expected a module path"));
		}

		// Get the kind.
		let kind = infer_module_kind(file_name)?;

		// Create the module.
		let module = tg::module::Data {
			kind,
			referent: tg::Referent::with_item(tg::module::data::Item::Path(path.to_owned())),
		};

		Ok(module)
	}
}

pub(crate) fn infer_module_kind(path: impl AsRef<Path>) -> tg::Result<tg::module::Kind> {
	let path = path.as_ref();
	if path.ends_with(".d.ts") {
		Ok(tg::module::Kind::Dts)
	} else if path.extension().is_some_and(|ext| ext == "ts") {
		Ok(tg::module::Kind::Ts)
	} else if path.extension().is_some_and(|ext| ext == "js") {
		Ok(tg::module::Kind::Js)
	} else {
		Err(tg::error!(%path = path.display(), "unknown or missing file extension"))
	}
}
