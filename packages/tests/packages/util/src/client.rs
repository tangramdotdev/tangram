use {std::path::Path, tangram_client::prelude::*};

/// Create a client for the server whose directory is at the path.
pub fn client(path: &Path) -> tg::Result<tg::Client> {
	let path = path.join("socket");
	let path = path
		.to_str()
		.ok_or_else(|| tg::error!(path = %path.display(), "invalid socket path"))?;
	let url = tangram_uri::Uri::builder()
		.scheme("http+unix")
		.authority(path)
		.path("")
		.build()
		.map_err(|error| tg::error!(!error, "failed to build the server URL"))?;
	let arg = tg::Arg {
		url: Some(url),
		..Default::default()
	};
	let client = tg::Client::new(arg)?;

	Ok(client)
}
