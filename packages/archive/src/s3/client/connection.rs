use {
	super::Client,
	std::{sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_uri::Uri,
	tokio::io::{AsyncRead, AsyncWrite},
};

pub(super) struct Connection {
	sender: hyper::client::conn::http1::SendRequest<tangram_http::body::Boxed>,
}

impl Connection {
	#[must_use]
	fn new(sender: hyper::client::conn::http1::SendRequest<tangram_http::body::Boxed>) -> Self {
		Self { sender }
	}

	pub(super) fn is_closed(&self) -> bool {
		self.sender.is_closed()
	}

	pub(super) async fn send(
		&mut self,
		request: http::Request<tangram_http::body::Boxed>,
	) -> tg::Result<http::Response<hyper::body::Incoming>> {
		self.sender
			.ready()
			.await
			.map_err(|error| tg::error!(!error, "failed to ready the S3 connection"))?;
		let response = self
			.sender
			.send_request(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the S3 request"))?;

		Ok(response)
	}
}

impl Client {
	pub(super) async fn connect(url: &Uri) -> tg::Result<Connection> {
		let host = url
			.host()
			.ok_or_else(|| tg::error!(%url, "the S3 URL has no host"))?;
		let port = url
			.port_or_known_default()
			.ok_or_else(|| tg::error!(%url, "the S3 URL has no port"))?
			.try_into()
			.map_err(|_| tg::error!(%url, "the S3 URL port is out of range"))?;
		match url.scheme() {
			Some("http") => {
				let stream = Self::connect_tcp(host, port).await?;
				Self::handshake_h1(stream).await
			},
			Some("https") => {
				let stream = Self::connect_tcp_tls(host, port).await?;
				Self::handshake_h1(stream).await
			},
			_ => Err(tg::error!(%url, "the S3 URL has an unsupported scheme")),
		}
	}

	async fn connect_tcp(host: &str, port: u16) -> tg::Result<tokio::net::TcpStream> {
		let addr = format!("{host}:{port}");
		tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(addr))
			.await
			.map_err(|_| tg::error!(%host, %port, "the S3 connection timed out"))?
			.map_err(
				|error| tg::error!(!error, %host, %port, "failed to create the S3 TCP connection"),
			)
	}

	async fn connect_tcp_tls(
		host: &str,
		port: u16,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		use rustls_platform_verifier::BuilderVerifierExt as _;

		let stream = Self::connect_tcp(host, port).await?;
		let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
			rustls::crypto::aws_lc_rs::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.unwrap()
		.with_platform_verifier()
		.map_err(|error| tg::error!(!error, "failed to create the S3 TLS config"))?
		.with_no_client_auth();
		config.alpn_protocols = vec![b"http/1.1".to_vec()];
		let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
		let server_name = rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(
			|error| tg::error!(!error, %host, "failed to create the S3 TLS server name"),
		)?;
		let stream = connector.connect(server_name, stream).await.map_err(
			|error| tg::error!(!error, %host, %port, "failed to create the S3 TLS connection"),
		)?;
		match stream.get_ref().1.alpn_protocol() {
			None | Some(b"http/1.1") => {},
			Some(protocol) => {
				return Err(tg::error!(
					protocol = %String::from_utf8_lossy(protocol),
					"the S3 server selected an unsupported protocol"
				));
			},
		}

		Ok(stream)
	}

	async fn handshake_h1<S>(stream: S) -> tg::Result<Connection>
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|error| tg::error!(!error, "failed to perform the S3 HTTP handshake"))?;
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "the S3 connection failed");
				})
				.ok();
		});
		sender
			.ready()
			.await
			.map_err(|error| tg::error!(!error, "failed to ready the S3 connection"))?;

		Ok(Connection::new(sender))
	}
}
