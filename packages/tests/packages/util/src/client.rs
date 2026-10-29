use {std::path::Path, tangram_client::prelude::*};

/// Create a client for the server at the URL.
pub fn client(url: &tangram_uri::Uri) -> tg::Result<tg::Client> {
	let arg = tg::Arg {
		url: Some(url.clone()),
		..Default::default()
	};
	let client = tg::Client::new(arg)?;

	Ok(client)
}

/// Create the URL of a Unix socket.
pub fn url(path: &Path) -> tg::Result<tangram_uri::Uri> {
	let path = path
		.to_str()
		.ok_or_else(|| tg::error!(path = %path.display(), "invalid socket path"))?;
	let url = tangram_uri::Uri::builder()
		.scheme("http+unix")
		.authority(path)
		.path("")
		.build()
		.map_err(|error| tg::error!(!error, "failed to build the server URL"))?;

	Ok(url)
}

/// Get the URL that a server listens on, which is its first configured listener or a socket in its directory.
pub fn server_url(config: &crate::Config, path: &Path) -> tg::Result<tangram_uri::Uri> {
	match config.value()["http"]["listeners"][0]["url"].as_str() {
		Some(url) => url
			.parse()
			.map_err(|error| tg::error!(!error, "invalid listener URL")),
		None => self::url(&path.join("socket")),
	}
}
