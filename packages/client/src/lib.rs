use crate as tg;
use futures::{Stream, TryFutureExt as _};
use std::{
	collections::VecDeque,
	ops::Deref,
	path::{Path, PathBuf},
	pin::Pin,
	str::FromStr,
	sync::Arc,
	time::Duration,
};
use tangram_http::Body;
use time::{Date, Month, OffsetDateTime, Time, format_description::well_known::Rfc3339};
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite},
	net::{TcpStream, UnixStream},
};
use tower::{Service as _, util::BoxCloneSyncService};
use tower_http::ServiceBuilderExt as _;
use url::Url;

pub use self::{
	artifact::Handle as Artifact,
	blob::Handle as Blob,
	branch::Handle as Branch,
	checksum::Checksum,
	command::Handle as Command,
	diagnostic::Diagnostic,
	directory::Handle as Directory,
	error::{Error, Result, ok},
	file::Handle as File,
	graph::Handle as Graph,
	handle::Handle,
	health::Health,
	id::Id,
	leaf::Handle as Leaf,
	location::Location,
	lockfile::Lockfile,
	module::{Import, Module},
	mutation::Mutation,
	object::Handle as Object,
	position::Position,
	process::Process,
	range::Range,
	reference::Reference,
	referent::Referent,
	symlink::Handle as Symlink,
	tag::Tag,
	template::Template,
	user::User,
	value::Value,
};

pub mod artifact;
pub mod blob;
pub mod branch;
pub mod checksum;
pub mod clean;
pub mod command;
pub mod compiler;
pub mod diagnostic;
pub mod directory;
pub mod error;
pub mod export;
pub mod file;
pub mod graph;
pub mod handle;
pub mod health;
pub mod id;
pub mod import;
pub mod index;
pub mod leaf;
pub mod location;
pub mod lockfile;
pub mod module;
pub mod mutation;
pub mod object;
pub mod package;
pub mod pipe;
pub mod position;
pub mod process;
pub mod progress;
pub mod pull;
pub mod push;
pub mod range;
pub mod reference;
pub mod referent;
pub mod remote;
pub mod symlink;
pub mod tag;
pub mod template;
pub mod user;
pub mod util;
pub mod value;

#[derive(Clone, Debug)]
pub struct Client(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	url: Url,
	sender: Arc<tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Body>>>>,
	service: Service,
}

type Service = BoxCloneSyncService<http::Request<Body>, http::Response<Body>, tg::Error>;

impl Client {
	#[must_use]
	pub fn new(url: Url) -> Self {
		let sender = Arc::new(tokio::sync::Mutex::<
			Option<hyper::client::conn::http2::SendRequest<Body>>,
		>::new(None));
		let service = tower::service_fn({
			let url = url.clone();
			let sender = sender.clone();
			move |request| {
				let url = url.clone();
				let sender = sender.clone();
				async move {
					let mut guard = sender.lock().await;
					let mut sender = match guard.as_ref() {
						Some(sender) if sender.is_ready() => sender.clone(),
						_ => {
							let sender = Self::connect_h2(&url).await?;
							guard.replace(sender.clone());
							sender
						},
					};
					drop(guard);
					sender
						.send_request(request)
						.map_ok(|response| response.map(Body::with_body))
						.map_err(|source| tg::error!(!source, "failed to send the request"))
						.await
				}
			}
		});
		let service = tower::ServiceBuilder::new()
			.layer(tangram_http::layer::tracing::TracingLayer::new())
			.insert_request_header_if_not_present(
				http::HeaderName::from_str("x-tg-compatibility-date").unwrap(),
				http::HeaderValue::from_str(&Self::compatibility_date().format(&Rfc3339).unwrap())
					.unwrap(),
			)
			.insert_request_header_if_not_present(
				http::HeaderName::from_str("x-tg-version").unwrap(),
				http::HeaderValue::from_str(&Self::version()).unwrap(),
			)
			.layer(tangram_http::layer::compression::RequestCompressionLayer::default())
			.layer(tangram_http::layer::compression::ResponseDecompressionLayer)
			.service(service);
		let service = Service::new(service);
		Self(Arc::new(Inner {
			url,
			sender,
			service,
		}))
	}

	pub fn with_env() -> tg::Result<Self> {
		let url = std::env::var("TANGRAM_URL")
			.map_err(|error| {
				error!(
					source = error,
					"failed to get the TANGRAM_URL environment variable"
				)
			})?
			.parse()
			.map_err(|error| {
				error!(
					source = error,
					"could not parse a URL from the TANGRAM_URL environment variable"
				)
			})?;
		Ok(Self::new(url))
	}

	#[must_use]
	pub fn url(&self) -> &Url {
		&self.url
	}

	pub async fn connect(&self) -> tg::Result<()> {
		let mut guard = self.sender.lock().await;
		match guard.as_ref() {
			Some(sender) if sender.is_ready() => (),
			_ => {
				let sender = Self::connect_h2(&self.url).await?;
				guard.replace(sender.clone());
			},
		}
		Ok(())
	}

	pub async fn disconnect(&self) -> tg::Result<()> {
		self.sender.lock().await.take();
		Ok(())
	}

	async fn connect_h1(url: &Url) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		match url.scheme() {
			"http+unix" => {
				let path = url.host_str().ok_or_else(|| tg::error!("invalid url"))?;
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, "invalid url"))?;
				let path = PathBuf::from(path.into_owned());
				Self::connect_unix_h1(&path).await
			},
			"http" => {
				let host = url.host_str().ok_or_else(|| tg::error!("invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				Self::connect_tcp_h1(host, port).await
			},
			"https" => {
				#[cfg(not(feature = "tls"))]
				{
					Err(tg::error!("tls is not enabled"))
				}
				#[cfg(feature = "tls")]
				{
					let host = url
						.domain()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
					let port = url
						.port_or_known_default()
						.ok_or_else(|| tg::error!(%url, "invalid url"))?;
					Self::connect_tcp_tls_h1(host, port).await
				}
			},
			_ => Err(tg::error!("invalid url")),
		}
	}

	async fn connect_h2(url: &Url) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		match url.scheme() {
			"http+unix" => {
				let path = url.host_str().ok_or_else(|| tg::error!("invalid url"))?;
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, %path, "invalid url"))?;
				let path = PathBuf::from(path.into_owned());
				Self::connect_unix_h2(&path).await
			},
			"http" => {
				let host = url.host_str().ok_or_else(|| tg::error!("invalid url"))?;
				let port = url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				Self::connect_tcp_h2(host, port).await
			},
			"https" => {
				#[cfg(not(feature = "tls"))]
				{
					Err(tg::error!("tls is not enabled"))
				}
				#[cfg(feature = "tls")]
				{
					let host = url.domain().ok_or_else(|| tg::error!("invalid url"))?;
					let port = url
						.port_or_known_default()
						.ok_or_else(|| tg::error!("invalid url"))?;
					Self::connect_tcp_tls_h2(host, port).await
				}
			},
			_ => Err(tg::error!("invalid url")),
		}
	}

	async fn connect_unix_h1(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
				.await
				.inspect_err(|error| {
					#[allow(unused)]
					use std::file;
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	async fn connect_unix_h2(
		path: &Path,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::Builder::new(executor)
			.max_concurrent_streams(None)
			.handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					#[allow(unused)]
					use std::file;
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	async fn connect_tcp_h1(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		// Connect via TCP.
		let stream = TcpStream::connect(format!("{host}:{port}"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
				.await
				.inspect_err(|error| {
					#[allow(unused)]
					use std::file;
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	async fn connect_tcp_h2(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		// Connect via TCP.
		let stream = TcpStream::connect(format!("{host}:{port}"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					#[allow(unused)]
					use std::file;
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	#[cfg(feature = "tls")]
	async fn connect_tcp_tls_h1(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Body>> {
		// Connect via TLS over TCP.
		let stream = Self::connect_tcp_tls(host, port, vec![b"http/1.1".into()]).await?;

		// Verify the negotiated protocol.
		let success = stream
			.get_ref()
			.1
			.alpn_protocol()
			.is_some_and(|protocol| protocol == b"http/1.1");
		if !success {
			return Err(tg::error!("failed to negotiate the protocol"));
		}

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
				.await
				.inspect_err(|error| {
					#[allow(unused)]
					use std::file;
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	#[cfg(feature = "tls")]
	async fn connect_tcp_tls_h2(
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Body>> {
		// Connect via TLS over TCP.
		let stream = Self::connect_tcp_tls(host, port, vec![b"h2".into()]).await?;

		// Verify the negotiated protocol.
		let success = stream
			.get_ref()
			.1
			.alpn_protocol()
			.is_some_and(|protocol| protocol == b"h2");
		if !success {
			return Err(tg::error!("failed to negotiate the protocol"));
		}

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the HTTP handshake"))?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					#[allow(unused)]
					use std::file;
					tracing::error!(error = ?error, "the connection failed");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.map_err(|source| tg::error!(!source, "failed to ready the sender"))?;

		Ok(sender)
	}

	#[cfg(feature = "tls")]
	async fn connect_tcp_tls(
		host: &str,
		port: u16,
		protocols: Vec<Vec<u8>>,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		// Connect via TCP.
		let stream = TcpStream::connect(format!("{host}:{port}"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Create the connector.
		let mut root_store = rustls::RootCertStore::empty();
		root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
		let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
			rustls::crypto::aws_lc_rs::default_provider(),
		))
		.with_safe_default_protocol_versions()
		.unwrap()
		.with_root_certificates(root_store)
		.with_no_client_auth();
		config.alpn_protocols = protocols;
		let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

		// Create the server name.
		let server_name = rustls::pki_types::ServerName::try_from(host.to_string().as_str())
			.map_err(|source| tg::error!(!source, "failed to create the server name"))?
			.to_owned();

		// Connect via TLS.
		let stream = connector
			.connect(server_name, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect"))?;

		Ok(stream)
	}

	async fn send(&self, request: http::Request<Body>) -> tg::Result<http::Response<Body>> {
		if request.body().try_clone().is_some() {
			self.send_with_retry(request).await
		} else {
			self.send_without_retry(request).await
		}
	}

	async fn send_with_retry(
		&self,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		let mut retries = VecDeque::from([100, 1000]);
		let (head, body) = request.into_parts();
		loop {
			let request = http::Request::from_parts(head.clone(), body.try_clone().unwrap());
			let result = self.send_without_retry(request).await;
			let is_error = result.is_err();
			let is_server_error =
				matches!(&result, Ok(response) if response.status().is_server_error());
			if is_error || is_server_error {
				if let Some(duration) = retries.pop_front() {
					let duration = Duration::from_millis(duration);
					tokio::time::sleep(duration).await;
					continue;
				}
			}
			return result;
		}
	}

	async fn send_without_retry(
		&self,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		let future = self.service.clone().call(request);
		let timeout = Duration::from_secs(60);
		let response = tokio::time::timeout(timeout, future)
			.await
			.map_err(|source| tg::error!(!source, "the request timed out"))?
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = response.map(Into::into);
		Ok(response)
	}

	fn compatibility_date() -> OffsetDateTime {
		OffsetDateTime::new_utc(
			Date::from_calendar_date(2025, Month::January, 1).unwrap(),
			Time::MIDNIGHT,
		)
	}

	fn version() -> String {
		let mut version = env!("CARGO_PKG_VERSION").to_owned();
		if let Some(commit) = option_env!("TANGRAM_CLI_COMMIT_HASH") {
			version.push('+');
			version.push_str(commit);
		}
		version
	}
}

impl tg::Handle for Client {
	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
			+ Send
			+ 'static,
		>,
	> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>
			+ Send
			+ 'static,
		>,
	> {
		self.check_out_artifact(id, arg)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> {
		self.create_blob(reader)
	}

	fn try_read_blob_stream(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::blob::read::Event>> + Send + 'static>,
		>,
	> {
		self.try_read_blob_stream(id, arg)
	}

	fn import(
		&self,
		arg: tg::import::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>,
	> {
		self.import(arg, stream)
	}

	fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>,
	> {
		self.export(arg, stream)
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.push(arg)
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.pull(arg)
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
	}

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_object(id, arg)
	}

	fn check_package(
		&self,
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<tg::package::check::Output>> {
		self.check_package(arg)
	}

	fn document_package(
		&self,
		arg: tg::package::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.document_package(arg)
	}

	fn format_package(
		&self,
		arg: tg::package::format::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.format_package(arg)
	}

	fn open_pipe(&self) -> impl Future<Output = tg::Result<tg::pipe::open::Output>> {
		self.open_pipe()
	}

	fn close_pipe(&self, id: &tg::pipe::Id) -> impl Future<Output = tg::Result<()>> {
		self.close_pipe(id)
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		self.read_pipe(id)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> {
		self.write_pipe(id, stream)
	}

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::spawn::Output>>> {
		self.try_spawn_process(arg)
	}

	fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		self.try_wait_process_future(id)
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::metadata::Output>>> {
		self.try_get_process_metadata(id)
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process(id)
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_process(id, arg)
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> {
		self.try_dequeue_process(arg)
	}

	fn try_start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<tg::process::start::Output>> {
		self.try_start_process(id, arg)
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> {
		self.heartbeat_process(id, arg)
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_status_stream(id)
	}

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::process::children::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_children_stream(id, arg)
	}

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_log_stream(id, arg)
	}

	fn try_post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<tg::process::log::post::Output>> {
		self.try_post_process_log(id, arg)
	}

	fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<tg::process::finish::Output>> {
		self.try_finish_process(id, arg)
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_process(id, arg)
	}

	fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::reference::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send {
		self.try_get_reference(reference)
	}

	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		self.list_remotes(arg)
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(name)
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_remote(name, arg)
	}

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		self.delete_remote(name)
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		self.health()
	}

	fn index(&self) -> impl Future<Output = tg::Result<()>> {
		self.index()
	}

	fn clean(&self) -> impl Future<Output = tg::Result<()>> {
		self.clean()
	}

	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		self.list_tags(arg)
	}

	fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(pattern)
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_tag(tag, arg)
	}

	fn delete_tag(&self, tag: &tg::Tag) -> impl Future<Output = tg::Result<()>> {
		self.delete_tag(tag)
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_user(token)
	}
}

impl Deref for Client {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

/// Get the host.
#[must_use]
pub fn host() -> &'static str {
	#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
	{
		"aarch64-darwin"
	}
	#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
	{
		"aarch64-linux"
	}
	#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
	{
		"x86_64-darwin"
	}
	#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
	{
		"x86_64-linux"
	}
}
