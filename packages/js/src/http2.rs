use {
	bytes::Bytes,
	dashmap::DashMap,
	http_body_util::{BodyExt as _, StreamBody},
	hyper::body::Incoming,
	percent_encoding::percent_decode_str,
	rustls_platform_verifier::BuilderVerifierExt as _,
	std::{
		path::{Path, PathBuf},
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tokio::sync::{Mutex, mpsc},
	tokio_stream::wrappers::ReceiverStream,
};

type BodyFrame = Result<http_body::Frame<Bytes>, std::io::Error>;
type RequestBody = StreamBody<ReceiverStream<BodyFrame>>;

#[derive(Default)]
pub(crate) struct Http2 {
	next: AtomicUsize,
	sessions: DashMap<usize, Arc<Session>>,
	streams: DashMap<usize, Arc<Stream>>,
}

struct Session {
	authority: String,
	scheme: String,
	sender: hyper::client::conn::http2::SendRequest<RequestBody>,
	streams: DashMap<usize, ()>,
}

struct Stream {
	event_tx: mpsc::Sender<StreamEvent>,
	events: Mutex<mpsc::Receiver<StreamEvent>>,
	request: Mutex<Option<mpsc::Sender<BodyFrame>>>,
	session: usize,
	token: usize,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ConnectOptions {
	#[serde(default)]
	port: Option<u16>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct RequestOptions {
	#[serde(default)]
	end_stream: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StreamEvent {
	Close,
	Data { bytes: Bytes },
	End,
	Error { message: String },
	Response { headers: Vec<(String, String)> },
	Trailers { headers: Vec<(String, String)> },
}

impl Http2 {
	fn token(&self) -> usize {
		self.next.fetch_add(1, Ordering::Relaxed) + 1
	}

	pub(crate) async fn connect(
		&self,
		authority: String,
		options: ConnectOptions,
	) -> tg::Result<usize> {
		if let Some(path) = authority.strip_prefix("http+unix://") {
			let path = unix_path_from_str(path)?;
			let stream = connect_unix(&path).await?;
			let sender = handshake(stream).await?;
			return Ok(self.insert_session("localhost".to_owned(), "http".to_owned(), sender));
		}

		let uri = authority
			.parse::<http::Uri>()
			.map_err(|error| tg::error!(!error, "invalid authority"))?;
		let scheme = uri
			.scheme_str()
			.ok_or_else(|| tg::error!("missing a URL scheme"))?;
		let (authority, scheme, sender) = match scheme {
			"http" | "https" => {
				let host = uri.host().ok_or_else(|| tg::error!("missing a URL host"))?;
				let port = options
					.port
					.or_else(|| uri.port_u16())
					.or(match scheme {
						"http" => Some(80),
						"https" => Some(443),
						_ => None,
					})
					.ok_or_else(|| tg::error!("unsupported URL scheme"))?;
				let authority = if uri.port_u16().is_some() {
					uri.authority()
						.ok_or_else(|| tg::error!("missing a URL authority"))?
						.as_str()
						.to_owned()
				} else {
					host.to_owned()
				};
				let stream = connect_tcp(host, port).await?;
				let sender = if scheme == "https" {
					let stream = connect_tls(host, stream).await?;
					verify_alpn_protocol(&stream)?;
					handshake(stream).await?
				} else {
					handshake(stream).await?
				};
				(authority, scheme.to_owned(), sender)
			},
			"http+unix" => {
				let path = unix_path(&uri)?;
				let stream = connect_unix(&path).await?;
				let sender = handshake(stream).await?;
				("localhost".to_owned(), "http".to_owned(), sender)
			},
			_ => return Err(tg::error!("unsupported URL scheme")),
		};

		Ok(self.insert_session(authority, scheme, sender))
	}

	fn insert_session(
		&self,
		authority: String,
		scheme: String,
		sender: hyper::client::conn::http2::SendRequest<RequestBody>,
	) -> usize {
		let token = self.token();
		let session = Session {
			authority,
			scheme,
			sender,
			streams: DashMap::new(),
		};
		self.sessions.insert(token, Arc::new(session));
		token
	}

	pub(crate) async fn session_request(
		&self,
		session: usize,
		headers: Vec<(String, String)>,
		options: RequestOptions,
	) -> tg::Result<usize> {
		let session_ref = self
			.sessions
			.get(&session)
			.ok_or_else(|| tg::error!("invalid HTTP/2 session"))?;
		let session_value = session_ref.value().clone();
		drop(session_ref);

		let (request_tx, request_rx) = mpsc::channel(16);
		let (event_tx, event_rx) = mpsc::channel(16);
		let request = build_request(&session_value, headers, ReceiverStream::new(request_rx))?;
		let token = self.token();
		let stream = Arc::new(Stream {
			event_tx,
			events: Mutex::new(event_rx),
			request: Mutex::new((!options.end_stream).then_some(request_tx)),
			session,
			token,
		});
		session_value.streams.insert(token, ());
		self.streams.insert(token, stream.clone());

		tokio::spawn(send_request(
			session_value,
			request,
			stream.event_tx.clone(),
		));

		Ok(token)
	}

	pub(crate) async fn stream_write(&self, stream: usize, bytes: Bytes) -> tg::Result<()> {
		let stream = self
			.streams
			.get(&stream)
			.ok_or_else(|| tg::error!("invalid HTTP/2 stream"))?
			.value()
			.clone();
		let request = stream.request.lock().await;
		send_request_body_frame(request.as_ref(), bytes).await
	}

	pub(crate) async fn stream_end(&self, stream: usize, bytes: Option<Bytes>) -> tg::Result<()> {
		let stream = self
			.streams
			.get(&stream)
			.ok_or_else(|| tg::error!("invalid HTTP/2 stream"))?
			.value()
			.clone();
		let mut request = stream.request.lock().await;
		if let Some(bytes) = bytes {
			send_request_body_frame(request.as_ref(), bytes).await?;
		}
		request.take();
		Ok(())
	}

	pub(crate) async fn stream_read(&self, stream: usize) -> tg::Result<Option<StreamEvent>> {
		let stream = self
			.streams
			.get(&stream)
			.ok_or_else(|| tg::error!("invalid HTTP/2 stream"))?
			.value()
			.clone();
		Ok(stream.events.lock().await.recv().await)
	}

	pub(crate) async fn stream_close(&self, stream: usize) -> tg::Result<()> {
		if let Some((_token, stream)) = self.streams.remove(&stream) {
			self.remove_stream_from_session(&stream);
			stream.close().await;
		}
		Ok(())
	}

	pub(crate) async fn session_close(&self, session: usize) -> tg::Result<()> {
		self.session_destroy(session, None).await
	}

	pub(crate) async fn session_destroy(
		&self,
		session: usize,
		_error: Option<String>,
	) -> tg::Result<()> {
		let Some((_token, session)) = self.sessions.remove(&session) else {
			return Ok(());
		};
		let streams = session
			.streams
			.iter()
			.map(|stream| *stream.key())
			.collect::<Vec<_>>();
		for stream in streams {
			if let Some((_token, stream)) = self.streams.remove(&stream) {
				stream.close().await;
			}
		}
		Ok(())
	}

	fn remove_stream_from_session(&self, stream: &Stream) {
		let session = self
			.sessions
			.get(&stream.session)
			.map(|session| session.value().clone());
		if let Some(session) = session {
			session.streams.remove(&stream.token);
		}
	}
}

impl Stream {
	async fn close(&self) {
		self.request.lock().await.take();
		self.event_tx.try_send(StreamEvent::Close).ok();
	}
}

async fn connect_tcp(host: &str, port: u16) -> tg::Result<tokio::net::TcpStream> {
	let addr = format!("{host}:{port}");
	tokio::time::timeout(
		Duration::from_secs(10),
		tokio::net::TcpStream::connect(addr),
	)
	.await
	.map_err(|_| tg::error!("connection timeout"))?
	.map_err(|error| tg::error!(!error, "failed to create the TCP connection"))
}

async fn connect_unix(path: &Path) -> tg::Result<tokio::net::UnixStream> {
	let resolved = tangram_util::io::unix::resolve(path).map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to resolve the socket path"),
	)?;
	tokio::net::UnixStream::connect(resolved.as_ref())
		.await
		.map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to connect to the socket"),
		)
}

async fn connect_tls(
	host: &str,
	stream: tokio::net::TcpStream,
) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
	let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
		rustls::crypto::aws_lc_rs::default_provider(),
	))
	.with_safe_default_protocol_versions()
	.unwrap()
	.with_platform_verifier()
	.map_err(|error| tg::error!(!error, "failed to create the TLS config"))?
	.with_no_client_auth();
	config.alpn_protocols = vec![b"h2".to_vec()];
	let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
	let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
		.map_err(|error| tg::error!(!error, "failed to create the server name"))?;
	connector
		.connect(server_name, stream)
		.await
		.map_err(|error| tg::error!(!error, "failed to connect with TLS"))
}

fn unix_path(uri: &http::Uri) -> tg::Result<PathBuf> {
	let path = if let Some(authority) = uri.authority() {
		authority.as_str()
	} else {
		uri.path()
	};
	unix_path_from_str(path)
}

fn unix_path_from_str(path: &str) -> tg::Result<PathBuf> {
	let path = percent_decode_str(path)
		.decode_utf8()
		.map_err(|error| tg::error!(!error, "invalid Unix socket path"))?;
	if path.is_empty() {
		return Err(tg::error!("missing a Unix socket path"));
	}
	Ok(PathBuf::from(path.as_ref()))
}

async fn handshake<S>(stream: S) -> tg::Result<hyper::client::conn::http2::SendRequest<RequestBody>>
where
	S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
	let executor = hyper_util::rt::TokioExecutor::new();
	let io = hyper_util::rt::TokioIo::new(stream);
	let (mut sender, connection) = hyper::client::conn::http2::Builder::new(executor)
		.max_concurrent_streams(None)
		.max_concurrent_reset_streams(usize::MAX)
		.handshake(io)
		.await
		.map_err(|error| tg::error!(!error, "failed to perform the HTTP/2 handshake"))?;
	tokio::spawn(async move {
		connection.await.ok();
	});
	sender
		.ready()
		.await
		.map_err(|error| tg::error!(!error, "failed to ready the HTTP/2 sender"))?;
	Ok(sender)
}

fn verify_alpn_protocol(
	stream: &tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
) -> tg::Result<()> {
	if stream
		.get_ref()
		.1
		.alpn_protocol()
		.is_some_and(|actual| actual == b"h2")
	{
		Ok(())
	} else {
		Err(tg::error!("failed to negotiate the HTTP/2 protocol"))
	}
}

fn build_request(
	session: &Session,
	headers: Vec<(String, String)>,
	body: ReceiverStream<BodyFrame>,
) -> tg::Result<http::Request<RequestBody>> {
	let mut method = http::Method::GET;
	let mut path = "/".to_owned();
	let mut authority = session.authority.clone();
	let mut scheme = session.scheme.clone();
	let mut regular_headers = Vec::new();
	for (name, value) in headers {
		match name.as_str() {
			":method" => {
				method = value
					.parse()
					.map_err(|error| tg::error!(!error, "invalid HTTP/2 method"))?;
			},
			":path" => path = value,
			":authority" => authority = value,
			":scheme" => scheme = value,
			_ => regular_headers.push((name, value)),
		}
	}
	let uri = http::Uri::builder()
		.scheme(scheme.as_str())
		.authority(authority.as_str())
		.path_and_query(path.as_str())
		.build()
		.map_err(|error| tg::error!(!error, "invalid HTTP/2 request URI"))?;
	let mut request = http::Request::builder()
		.method(method)
		.uri(uri)
		.body(StreamBody::new(body))
		.map_err(|error| tg::error!(!error, "invalid HTTP/2 request"))?;
	for (name, value) in regular_headers {
		let name = http::HeaderName::try_from(name)
			.map_err(|error| tg::error!(!error, "invalid HTTP/2 header name"))?;
		let value = http::HeaderValue::try_from(value)
			.map_err(|error| tg::error!(!error, "invalid HTTP/2 header value"))?;
		request.headers_mut().append(name, value);
	}
	Ok(request)
}

async fn send_request(
	session: Arc<Session>,
	request: http::Request<RequestBody>,
	event_tx: mpsc::Sender<StreamEvent>,
) {
	let mut sender = session.sender.clone();
	let result = async {
		sender
			.ready()
			.await
			.map_err(|error| tg::error!(!error, "failed to ready the HTTP/2 sender"))?;
		let response = sender
			.send_request(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the HTTP/2 request"))?;
		send_response_events(response, event_tx.clone()).await?;
		Ok::<_, tg::Error>(())
	}
	.await;
	if let Err(error) = result {
		event_tx
			.send(StreamEvent::Error {
				message: error.to_string(),
			})
			.await
			.ok();
	}
	event_tx.send(StreamEvent::Close).await.ok();
}

async fn send_response_events(
	response: http::Response<Incoming>,
	event_tx: mpsc::Sender<StreamEvent>,
) -> tg::Result<()> {
	let (parts, mut body) = response.into_parts();
	let mut headers = headers_to_vec(&parts.headers);
	headers.push((":status".to_owned(), parts.status.as_u16().to_string()));
	event_tx
		.send(StreamEvent::Response { headers })
		.await
		.map_err(|_| tg::error!("the HTTP/2 stream event channel is closed"))?;
	while let Some(frame) = body.frame().await {
		let frame =
			frame.map_err(|error| tg::error!(!error, "failed to read the HTTP/2 response body"))?;
		if let Some(data) = frame.data_ref() {
			event_tx
				.send(StreamEvent::Data {
					bytes: data.clone(),
				})
				.await
				.map_err(|_| tg::error!("the HTTP/2 stream event channel is closed"))?;
		}
		if let Some(trailers) = frame.trailers_ref() {
			event_tx
				.send(StreamEvent::Trailers {
					headers: headers_to_vec(trailers),
				})
				.await
				.map_err(|_| tg::error!("the HTTP/2 stream event channel is closed"))?;
		}
	}
	event_tx
		.send(StreamEvent::End)
		.await
		.map_err(|_| tg::error!("the HTTP/2 stream event channel is closed"))?;
	Ok(())
}

fn headers_to_vec(headers: &http::HeaderMap) -> Vec<(String, String)> {
	headers
		.iter()
		.map(|(name, value)| {
			(
				name.as_str().to_owned(),
				value.to_str().unwrap_or_default().to_owned(),
			)
		})
		.collect()
}

async fn send_request_body_frame(
	sender: Option<&mpsc::Sender<BodyFrame>>,
	bytes: Bytes,
) -> tg::Result<()> {
	let Some(sender) = sender else {
		return Err(tg::error!("the HTTP/2 stream request body is closed"));
	};
	sender
		.send(Ok(http_body::Frame::data(bytes)))
		.await
		.map_err(|_| tg::error!("the HTTP/2 stream request body is closed"))
}
