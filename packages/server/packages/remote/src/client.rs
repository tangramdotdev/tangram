use {crate::Component, tangram_client::prelude::*, tangram_server_core::Server, tangram_uri::Uri};

impl Component {
	pub(crate) fn get_or_create_remote_client(
		&self,
		server: &Server,
		url: Uri,
	) -> tg::Result<tg::Client> {
		if let Some(client) = self.clients.get(&url) {
			return Ok(client.clone());
		}
		let client = Self::create_remote_client(server, url.clone())?;
		self.clients.insert(url, client.clone());
		Ok(client)
	}

	fn create_remote_client(server: &Server, url: Uri) -> tg::Result<tg::Client> {
		tg::Client::new(tg::Arg {
			http: tg::Http {
				http2: server.config().http.http2,
				..Default::default()
			},
			url: Some(url),
			version: Some(server.version.clone()),
			token: None,
			pool: server.config().remote_client.pool,
			stream_retry: Server::client_stream_retry(),
			sync: tg::sync::Config {
				max_frame_size: server.config().sync.max_frame_size,
			},
			..Default::default()
		})
	}
}
