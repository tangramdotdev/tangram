use {data_encoding::BASE64URL_NOPAD, std::path::Path, tangram_client::prelude::*};

pub(super) fn module_path(module: &tg::module::Data) -> String {
	if let (tg::module::Kind::TypeScriptDeclaration, tg::module::data::Source::Path(path)) =
		(&module.kind, &module.referent.node)
	{
		return format!(
			"/__library__/{}",
			path.to_string_lossy().trim_start_matches("./")
		);
	}
	let identity = BASE64URL_NOPAD.encode(module.without_token().to_string().as_bytes());
	let extension = if module.kind == tg::module::Kind::JavaScript {
		"js"
	} else {
		"ts"
	};
	let path = format!("/__runtime__/{identity}/index.{extension}");

	path
}

pub(super) fn try_module_from_path(path: &str) -> tg::Result<Option<tg::module::Data>> {
	if let Some(path) = path.strip_prefix("/__library__/") {
		let referent = tg::Referent::with_node(tg::module::data::Source::Path(path.into()));
		let module = tg::module::Data {
			kind: tg::module::Kind::TypeScriptDeclaration,
			referent,
		};
		return Ok(Some(module));
	}
	let Some(path) = path.strip_prefix("/__runtime__/") else {
		return Ok(None);
	};
	let Some((identity, filename)) = path.split_once('/') else {
		return Ok(None);
	};
	if !matches!(filename, "index.js" | "index.ts") {
		return Ok(None);
	}
	let identity = BASE64URL_NOPAD
		.decode(identity.as_bytes())
		.map_err(|error| tg::error!(!error, "invalid encoding for a tsrs module path"))?;
	let identity = std::str::from_utf8(&identity)
		.map_err(|error| tg::error!(!error, "invalid identity in a tsrs module path"))?;
	let module = identity
		.parse::<tg::module::Data>()
		.map_err(|error| tg::error!(!error, "failed to decode the tsrs module identity"))?
		.without_token();

	Ok(Some(module))
}

pub(super) fn try_library_file(path: &str) -> Option<&'static include_dir::File<'static>> {
	let path = Path::new(path);
	let relative = path.strip_prefix("/__library__").ok();
	let name = path.file_name()?.to_str()?;
	let path = relative.or_else(|| name.starts_with("lib.").then(|| Path::new(name)))?;
	crate::LIBRARY.get_file(path)
}
