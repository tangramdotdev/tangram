use self::util::http::{Incoming, Outgoing};
pub use self::{
	artifact::Artifact,
	blob::Blob,
	branch::Branch,
	build::Build,
	checksum::Checksum,
	dependency::Dependency,
	diagnostic::Diagnostic,
	directory::Directory,
	document::Document,
	error::{ok, Error, Result},
	file::File,
	handle::Handle,
	id::Id,
	import::Import,
	leaf::Leaf,
	location::Location,
	lock::Lock,
	module::Module,
	mutation::Mutation,
	object::Handle as Object,
	path::Path,
	position::Position,
	range::Range,
	server::Health,
	symlink::Symlink,
	target::Target,
	template::Template,
	user::User,
	value::Value,
};
use crate as tg;
use bytes::Bytes;
use futures::{Future, Stream};
use std::{path::PathBuf, sync::Arc};
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite},
	net::{TcpStream, UnixStream},
};
use url::Url;

pub mod artifact;
pub mod blob;
pub mod branch;
pub mod build;
pub mod checksum;
pub mod dependency;
pub mod diagnostic;
pub mod directory;
pub mod document;
pub mod error;
pub mod file;
pub mod handle;
pub mod id;
pub mod import;
pub mod language;
pub mod leaf;
pub mod location;
pub mod lock;
pub mod module;
pub mod mutation;
pub mod object;
pub mod package;
pub mod path;
pub mod position;
pub mod range;
pub mod runtime;
pub mod server;
pub mod symlink;
pub mod target;
pub mod template;
pub mod user;
mod util;
pub mod value;

#[derive(Debug, Clone)]
pub struct Client(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	url: Url,
	sender: tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Outgoing>>>,
	token: Option<String>,
}

pub struct Builder {
	url: Url,
	token: Option<String>,
}

impl Client {
	#[must_use]
	pub fn new(url: Url, token: Option<String>) -> Self {
		let sender = tokio::sync::Mutex::new(None);
		Self(Arc::new(Inner { url, sender, token }))
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
		let token = None;
		Ok(Self::new(url, token))
	}

	#[must_use]
	pub fn url(&self) -> &Url {
		&self.url
	}

	pub async fn connect(&self) -> tg::Result<()> {
		self.sender().await.map(|_| ())
	}

	pub async fn disconnect(&self) -> tg::Result<()> {
		self.sender.lock().await.take();
		Ok(())
	}

	async fn sender(&self) -> tg::Result<hyper::client::conn::http2::SendRequest<Outgoing>> {
		if let Some(sender) = self.sender.lock().await.as_ref().cloned() {
			if sender.is_ready() {
				return Ok(sender);
			}
		}
		let mut sender_guard = self.sender.lock().await;
		let sender = self.connect_h2().await?;
		sender_guard.replace(sender.clone());
		Ok(sender)
	}

	async fn connect_h1(&self) -> tg::Result<hyper::client::conn::http1::SendRequest<Outgoing>> {
		match self.url.scheme() {
			"http+unix" => {
				let path = self
					.url
					.host_str()
					.ok_or_else(|| tg::error!("invalid url"))?;
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, "invalid url"))?;
				let path = PathBuf::from(path.into_owned());
				self.connect_unix_h1(&path).await
			},
			"http" => {
				let host = self
					.url
					.host_str()
					.ok_or_else(|| tg::error!("invalid url"))?;
				let port = self
					.url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				self.connect_tcp_h1(host, port).await
			},
			"https" => {
				#[cfg(not(feature = "tls"))]
				{
					Err(tg::error!("tls is not enabled"))
				}
				#[cfg(feature = "tls")]
				{
					let host = self.url.domain().ok_or_else(|| tg::error!("invalid url"))?;
					let port = self
						.url
						.port_or_known_default()
						.ok_or_else(|| tg::error!("invalid url"))?;
					self.connect_tcp_tls_h1(host, port).await
				}
			},
			_ => Err(tg::error!("invalid url")),
		}
	}

	async fn connect_h2(&self) -> tg::Result<hyper::client::conn::http2::SendRequest<Outgoing>> {
		match self.url.scheme() {
			"http+unix" => {
				let path = self
					.url
					.host_str()
					.ok_or_else(|| tg::error!("invalid url"))?;
				let path = urlencoding::decode(path)
					.map_err(|source| tg::error!(!source, "invalid url"))?;
				let path = PathBuf::from(path.into_owned());
				self.connect_unix_h2(&path).await
			},
			"http" => {
				let host = self
					.url
					.host_str()
					.ok_or_else(|| tg::error!("invalid url"))?;
				let port = self
					.url
					.port_or_known_default()
					.ok_or_else(|| tg::error!("invalid url"))?;
				self.connect_tcp_h2(host, port).await
			},
			"https" => {
				#[cfg(not(feature = "tls"))]
				{
					Err(tg::error!("tls is not enabled"))
				}
				#[cfg(feature = "tls")]
				{
					let host = self.url.domain().ok_or_else(|| tg::error!("invalid url"))?;
					let port = self
						.url
						.port_or_known_default()
						.ok_or_else(|| tg::error!("invalid url"))?;
					self.connect_tcp_tls_h2(host, port).await
				}
			},
			_ => Err(tg::error!("invalid url")),
		}
	}

	async fn connect_unix_h1(
		&self,
		path: &std::path::Path,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Outgoing>> {
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
		&self,
		path: &std::path::Path,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Outgoing>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

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
		&self,
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Outgoing>> {
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
		&self,
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Outgoing>> {
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
		&self,
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http1::SendRequest<Outgoing>> {
		// Connect via TLS over TCP.
		let stream = self.connect_tcp_tls(host, port).await?;

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
		&self,
		host: &str,
		port: u16,
	) -> tg::Result<hyper::client::conn::http2::SendRequest<Outgoing>> {
		// Connect via TLS over TCP.
		let stream = self.connect_tcp_tls(host, port).await?;

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
		&self,
		host: &str,
		port: u16,
	) -> tg::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		// Connect via TCP.
		let stream = TcpStream::connect(format!("{host}:{port}"))
			.await
			.map_err(|source| tg::error!(!source, "failed to create the TCP connection"))?;

		// Create the connector.
		let mut root_store = rustls::RootCertStore::empty();
		root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
		let mut config = rustls::ClientConfig::builder()
			.with_root_certificates(root_store)
			.with_no_client_auth();
		config.alpn_protocols = vec!["h2".into()];
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

		// Verify the negotiated protocol.
		if !stream
			.get_ref()
			.1
			.alpn_protocol()
			.map_or(false, |protocol| protocol == b"h2")
		{
			return Err(tg::error!("failed to negotiate the HTTP/2 protocol"));
		}

		Ok(stream)
	}

	async fn send(&self, request: http::Request<Outgoing>) -> tg::Result<http::Response<Incoming>> {
		self.sender()
			.await?
			.send_request(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))
	}
}

impl Builder {
	#[must_use]
	pub fn new(url: Url) -> Self {
		Self { url, token: None }
	}

	#[must_use]
	pub fn token(mut self, token: Option<String>) -> Self {
		self.token = token;
		self
	}

	#[must_use]
	pub fn build(self) -> Client {
		let url = self.url;
		let token = self.token;
		Client::new(url, token)
	}
}

impl tg::Handle for Client {
	type Transaction<'a> = ();

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::ArchiveArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ArchiveOutput>> {
		self.archive_artifact(id, arg)
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::ExtractArg,
	) -> impl Future<Output = tg::Result<tg::artifact::ExtractOutput>> {
		self.extract_artifact(arg)
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::BundleOutput>> {
		self.bundle_artifact(id)
	}

	fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckInOutput>> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> impl Future<Output = tg::Result<tg::artifact::CheckOutOutput>> {
		self.check_out_artifact(id, arg)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		self.create_blob(reader, transaction)
	}

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::CompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::CompressOutput>> {
		self.compress_blob(id, arg)
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::DecompressArg,
	) -> impl Future<Output = tg::Result<tg::blob::DecompressOutput>> {
		self.decompress_blob(id, arg)
	}

	fn list_builds(
		&self,
		arg: tg::build::ListArg,
	) -> impl Future<Output = tg::Result<tg::build::ListOutput>> {
		self.list_builds(arg)
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::build::GetOutput>>> {
		self.try_get_build(id, arg)
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_build(id, arg)
	}

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_build(id)
	}

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_build(id)
	}

	fn get_or_create_build(
		&self,
		arg: tg::build::GetOrCreateArg,
	) -> impl Future<Output = tg::Result<tg::build::GetOrCreateOutput>> {
		self.get_or_create_build(arg)
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::DequeueArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<tg::build::DequeueOutput>>> {
		self.try_dequeue_build(arg, stop)
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<Option<impl Stream<Item = Result<tg::build::Status>> + Send + 'static>>,
	> {
		self.try_get_build_status(id, arg, stop)
	}

	fn set_build_status(
		&self,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> impl Future<Output = tg::Result<()>> {
		self.set_build_status(id, status)
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_children(id, arg, stop)
	}

	fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_child(build_id, child_id)
	}

	fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::log::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_log(id, arg, stop)
	}

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_log(id, bytes)
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> impl Future<Output = tg::Result<Option<Option<tg::build::Outcome>>>> {
		self.try_get_build_outcome(id, arg, stop)
	}

	fn set_build_outcome(
		&self,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> impl Future<Output = tg::Result<()>> {
		self.set_build_outcome(id, outcome)
	}

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> {
		self.format(text)
	}

	fn lsp(
		&self,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::GetOutput>>> {
		self.try_get_object(id)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::PutArg,
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::PutOutput>> {
		self.put_object(id, arg, transaction)
	}

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_object(id)
	}

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_object(id)
	}

	fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> impl Future<Output = tg::Result<tg::package::SearchOutput>> {
		self.search_packages(arg)
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> impl Future<Output = tg::Result<Option<tg::package::GetOutput>>> {
		self.try_get_package(dependency, arg)
	}

	fn check_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> {
		self.check_package(dependency)
	}

	fn format_package(&self, dependency: &tg::Dependency) -> impl Future<Output = tg::Result<()>> {
		self.format_package(dependency)
	}

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> {
		self.try_get_package_doc(dependency)
	}

	fn get_package_outdated(
		&self,
		arg: &tg::Dependency,
	) -> impl Future<Output = tg::Result<tg::package::OutdatedOutput>> {
		self.get_package_outdated(arg)
	}

	fn publish_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> {
		self.publish_package(id)
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> {
		self.try_get_package_versions(dependency)
	}

	fn yank_package(&self, id: &tg::directory::Id) -> impl Future<Output = tg::Result<()>> {
		self.yank_package(id)
	}

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.get_js_runtime_doc()
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		self.health()
	}

	fn clean(&self) -> impl Future<Output = tg::Result<()>> {
		self.clean()
	}

	fn stop(&self) -> impl Future<Output = tg::Result<()>> {
		self.stop()
	}

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_user(token)
	}
}

impl std::ops::Deref for Client {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
