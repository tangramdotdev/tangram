use crate as tg;
use bytes::Bytes;
use futures::{Future, Stream};
use std::{path::PathBuf, sync::Arc};
use tangram_http::{Incoming, Outgoing};
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite},
	net::{TcpStream, UnixStream},
};
use url::Url;

mod util;

pub use self::{
	artifact::Artifact,
	blob::Blob,
	branch::Branch,
	build::Build,
	checksum::Checksum,
	dependency::Dependency,
	diagnostic::Diagnostic,
	directory::Directory,
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

pub mod artifact;
pub mod blob;
pub mod branch;
pub mod build;
pub mod checksum;
pub mod compiler;
pub mod dependency;
pub mod diagnostic;
pub mod directory;
pub mod error;
pub mod file;
pub mod handle;
pub mod id;
pub mod import;
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
pub mod root;
pub mod runtime;
pub mod server;
pub mod symlink;
pub mod target;
pub mod template;
pub mod user;
pub mod value;

#[derive(Debug, Clone)]
pub struct Client(Arc<Inner>);

#[derive(Debug)]
pub struct Inner {
	url: Url,
	sender: tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Outgoing>>>,
}

impl Client {
	#[must_use]
	pub fn new(url: Url) -> Self {
		let sender = tokio::sync::Mutex::new(None);
		Self(Arc::new(Inner { url, sender }))
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
		self.sender().await.map(|_| ())
	}

	pub async fn connected(&self) -> bool {
		self.0
			.sender
			.lock()
			.await
			.as_ref()
			.is_some_and(hyper::client::conn::http2::SendRequest::is_ready)
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
		let stream = self
			.connect_tcp_tls(host, port, vec![b"http/1.1".into()])
			.await?;

		// Verify the negotiated protocol.
		let success = stream
			.get_ref()
			.1
			.alpn_protocol()
			.map_or(false, |protocol| protocol == b"http/1.1");
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
		let stream = self.connect_tcp_tls(host, port, vec![b"h2".into()]).await?;

		// Verify the negotiated protocol.
		let success = stream
			.get_ref()
			.1
			.alpn_protocol()
			.map_or(false, |protocol| protocol == b"h2");
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
		protocols: Vec<Vec<u8>>,
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

	async fn send(&self, request: http::Request<Outgoing>) -> tg::Result<http::Response<Incoming>> {
		self.sender()
			.await?
			.send_request(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))
	}
}

impl tg::Handle for Client {
	type Transaction<'a> = ();

	fn archive_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::archive::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::archive::Output>> {
		self.archive_artifact(id, arg)
	}

	fn extract_artifact(
		&self,
		arg: tg::artifact::extract::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::extract::Output>> {
		self.extract_artifact(arg)
	}

	fn bundle_artifact(
		&self,
		id: &tg::artifact::Id,
	) -> impl Future<Output = tg::Result<tg::artifact::bundle::Output>> {
		self.bundle_artifact(id)
	}

	fn checksum_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checksum::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checksum::Output>> {
		self.checksum_artifact(id, arg)
	}

	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkin::Output>> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkout::Output>> {
		self.check_out_artifact(id, arg)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::Id>> {
		self.create_blob(reader)
	}

	fn compress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::compress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::compress::Output>> {
		self.compress_blob(id, arg)
	}

	fn decompress_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::decompress::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::decompress::Output>> {
		self.decompress_blob(id, arg)
	}

	fn download_blob(
		&self,
		arg: tg::blob::download::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::download::Output>> {
		self.download_blob(arg)
	}

	fn checksum_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::checksum::Arg,
	) -> impl Future<Output = tg::Result<tg::blob::checksum::Output>> {
		self.checksum_blob(id, arg)
	}

	fn list_builds(
		&self,
		arg: tg::build::list::Arg,
	) -> impl Future<Output = tg::Result<tg::build::list::Output>> {
		self.list_builds(arg)
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> {
		self.try_get_build(id)
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_build(id, arg)
	}

	fn push_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_build(id)
	}

	fn pull_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_build(id)
	}

	fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::build::dequeue::Output>>> {
		self.try_dequeue_build(arg)
	}

	fn try_start_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		self.try_start_build(id)
	}

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> {
		self.heartbeat_build(id)
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> impl Future<
		Output = tg::Result<Option<impl Stream<Item = Result<tg::build::Status>> + Send + 'static>>,
	> {
		self.try_get_build_status(id, arg)
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::children::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_children(id, arg)
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
		arg: tg::build::log::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::log::Chunk>> + Send + 'static>,
		>,
	> {
		self.try_get_build_log(id, arg)
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
		arg: tg::build::outcome::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + 'static>,
		>,
	> {
		self.try_get_build_outcome(id, arg)
	}

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.finish_build(id, arg)
	}

	fn touch_build(&self, id: &tg::build::Id) -> impl Future<Output = tg::Result<()>> {
		self.touch_build(id)
	}

	fn format(&self, text: String) -> impl Future<Output = tg::Result<String>> {
		self.format(text)
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
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
		transaction: Option<&Self::Transaction<'_>>,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.put_object(id, arg, transaction)
	}

	fn push_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.push_object(id)
	}

	fn pull_object(&self, id: &tg::object::Id) -> impl Future<Output = tg::Result<()>> {
		self.pull_object(id)
	}

	fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> impl Future<Output = tg::Result<tg::package::list::Output>> {
		self.list_packages(arg)
	}

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::package::get::Output>>> {
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
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> {
		self.get_package_outdated(arg)
	}

	fn publish_package(&self, id: &tg::artifact::Id) -> impl Future<Output = tg::Result<()>> {
		self.publish_package(id)
	}

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> {
		self.try_get_package_versions(dependency)
	}

	fn yank_package(&self, id: &tg::artifact::Id) -> impl Future<Output = tg::Result<()>> {
		self.yank_package(id)
	}

	fn list_roots(
		&self,
		arg: tg::root::list::Arg,
	) -> impl Future<Output = tg::Result<tg::root::list::Output>> {
		self.list_roots(arg)
	}

	fn try_get_root(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::root::get::Output>>> {
		self.try_get_root(name)
	}

	fn put_root(
		&self,
		name: &str,
		arg: tg::root::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_root(name, arg)
	}

	fn delete_root(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		self.delete_root(name)
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

	fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<tg::target::build::Output>> {
		self.build_target(id, arg)
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
