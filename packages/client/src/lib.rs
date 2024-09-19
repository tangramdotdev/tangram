use crate as tg;
use futures::{Future, FutureExt as _, Stream};
use std::{
	collections::VecDeque,
	path::{Path, PathBuf},
	sync::Arc,
};
use tangram_http::{Incoming, Outgoing};
use tokio::{
	io::{AsyncBufRead, AsyncRead, AsyncWrite},
	net::{TcpStream, UnixStream},
};
use url::Url;

pub use tangram_either::{for_both, Either};

pub use self::{
	artifact::Artifact,
	blob::Blob,
	branch::Branch,
	build::Build,
	checksum::Checksum,
	diagnostic::Diagnostic,
	directory::Directory,
	error::{ok, Error, Result},
	file::File,
	graph::Graph,
	handle::Handle,
	id::Id,
	import::Import,
	leaf::Leaf,
	location::Location,
	lockfile::Lockfile,
	module::Module,
	mutation::Mutation,
	object::Handle as Object,
	position::Position,
	progress::Progress,
	range::Range,
	reference::Reference,
	symlink::Symlink,
	tag::Tag,
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
pub mod diagnostic;
pub mod directory;
pub mod error;
pub mod file;
pub mod graph;
pub mod handle;
pub mod id;
pub mod import;
pub mod leaf;
pub mod location;
pub mod lockfile;
pub mod module;
pub mod mutation;
pub mod object;
pub mod package;
pub mod path;
pub mod position;
pub mod progress;
pub mod range;
pub mod reference;
pub mod remote;
pub mod runtime;
pub mod server;
pub mod symlink;
pub mod tag;
pub mod target;
pub mod template;
pub mod user;
pub mod util;
pub mod value;

#[derive(Clone, Debug)]
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
		self.sender().boxed().await.map(|_| ())
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
		path: &Path,
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
		path: &Path,
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

	async fn send(&self, request: http::Request<Outgoing>) -> tg::Result<http::Response<Incoming>> {
		if request.body().try_clone().is_some() {
			self.send_with_retry(request).await
		} else {
			self.send_without_retry(request).await
		}
	}

	async fn send_with_retry(
		&self,
		request: http::Request<Outgoing>,
	) -> tg::Result<http::Response<Incoming>> {
		let mut retries = VecDeque::from([100, 1000]);
		let (head, body) = request.into_parts();
		loop {
			let request = http::Request::from_parts(head.clone(), body.try_clone().unwrap());
			let result = self
				.sender()
				.boxed()
				.await?
				.send_request(request)
				.await
				.map_err(|source| tg::error!(!source, "failed to send the request"));
			let is_error = result.is_err();
			let is_server_error =
				matches!(&result, Ok(response) if response.status().is_server_error());
			if is_error || is_server_error {
				if let Some(duration) = retries.pop_front() {
					let duration = std::time::Duration::from_millis(duration);
					tokio::time::sleep(duration).await;
					continue;
				}
			}
			return result;
		}
	}

	async fn send_without_retry(
		&self,
		request: http::Request<Outgoing>,
	) -> tg::Result<http::Response<Incoming>> {
		self.sender()
			.boxed()
			.await?
			.send_request(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))
	}
}

impl tg::Handle for Client {
	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::Progress<tg::artifact::Id>>> + Send + 'static,
		>,
	> {
		self.check_in_artifact(arg)
	}

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<PathBuf>>> + Send + 'static>,
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
	) -> impl Future<Output = tg::Result<tg::build::put::Output>> {
		self.put_build(id, arg)
	}

	fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		self.push_build(id, arg)
	}

	fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		self.pull_build(id, arg)
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
		arg: tg::build::start::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		self.try_start_build(id, arg)
	}

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> {
		self.heartbeat_build(id, arg)
	}

	fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::status::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_build_status_stream(id)
	}

	fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::children::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_build_children_stream(id, arg)
	}

	fn add_build_child(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_child(id, arg)
	}

	fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = Result<tg::build::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_build_log_stream(id, arg)
	}

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_build_log(id, arg)
	}

	fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + 'static>,
		>,
	> {
		self.try_get_build_outcome_future(id)
	}

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		self.finish_build(id, arg)
	}

	fn touch_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_build(id, arg)
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
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.put_object(id, arg)
	}

	fn push_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		self.push_object(id, arg)
	}

	fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static>,
	> {
		self.pull_object(id, arg)
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

	fn try_get_reference(
		&self,
		reference: &tg::Reference,
	) -> impl Future<Output = tg::Result<Option<tg::reference::get::Output>>> + Send {
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

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.get_js_runtime_doc()
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> {
		self.health()
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

	fn try_build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::target::build::Output>>> {
		self.try_build_target(id, arg)
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
