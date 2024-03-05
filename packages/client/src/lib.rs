pub use self::{
	artifact::Artifact, blob::Blob, branch::Branch, build::Build, checksum::Checksum,
	dependency::Dependency, diagnostic::Diagnostic, directory::Directory, document::Document,
	file::File, handle::Handle, id::Id, import::Import, leaf::Leaf, location::Location, lock::Lock,
	module::Module, mutation::Mutation, object::Handle as Object, path::Path, position::Position,
	range::Range, server::Health, symlink::Symlink, target::Target, template::Template,
	triple::Triple, user::Login, user::User, value::Value,
};
use crate as tg;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::{path::PathBuf, sync::Arc};
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::empty;
use tokio::{
	io::{AsyncRead, AsyncWrite},
	net::{TcpStream, UnixStream},
};
use url::Url;

pub mod artifact;
pub mod blob;
pub mod branch;
pub mod build;
pub mod bundle;
pub mod checksum;
pub mod dependency;
pub mod diagnostic;
pub mod directory;
pub mod document;
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
pub mod server;
pub mod symlink;
pub mod target;
pub mod template;
pub mod triple;
pub mod user;
mod util;
pub mod value;

#[derive(Debug, Clone)]
pub struct Client {
	inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
	address: Address,
	build: Option<tg::build::Id>,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	sender: tokio::sync::Mutex<
		Option<hyper::client::conn::http2::SendRequest<tangram_util::http::Outgoing>>,
	>,
	tls: bool,
	user: Option<User>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Address {
	Unix(PathBuf),
	Inet(Inet),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Inet {
	pub host: Host,
	pub port: u16,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Host {
	Ip(std::net::IpAddr),
	Domain(String),
}

pub struct Builder {
	address: Address,
	build: Option<build::Id>,
	tls: Option<bool>,
	user: Option<User>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Runtime {
	pub address: Address,
	pub build: build::Id,
}

impl Client {
	pub fn with_runtime() -> Result<Self> {
		let json = std::env::var("TANGRAM_RUNTIME")
			.wrap_err("Failed to get the TANGRAM_RUNTIME environment variable.")?;
		let runtime = serde_json::from_str::<Runtime>(&json)
			.wrap_err("Failed to deserialize the TANGRAM_RUNTIME environment variable.")?;
		let address = runtime.address;
		let build = Some(runtime.build);
		let file_descriptor_semaphore = tokio::sync::Semaphore::new(16);
		let sender = tokio::sync::Mutex::new(None);
		let tls = false;
		let user = None;
		let inner = Arc::new(Inner {
			address,
			build,
			file_descriptor_semaphore,
			sender,
			tls,
			user,
		});
		let client = Client { inner };
		Ok(client)
	}

	#[must_use]
	pub fn build(&self) -> Option<&build::Id> {
		self.inner.build.as_ref()
	}

	pub async fn connect(&self) -> Result<()> {
		self.sender().await.map(|_| ())
	}

	pub async fn disconnect(&self) -> Result<()> {
		self.inner.sender.lock().await.take();
		Ok(())
	}

	async fn sender(
		&self,
	) -> Result<hyper::client::conn::http2::SendRequest<tangram_util::http::Outgoing>> {
		if let Some(sender) = self.inner.sender.lock().await.as_ref().cloned() {
			if sender.is_ready() {
				return Ok(sender);
			}
		}
		let mut sender_guard = self.inner.sender.lock().await;
		let sender = self.connect_h2().await?;
		sender_guard.replace(sender.clone());
		Ok(sender)
	}

	async fn connect_h1(
		&self,
	) -> Result<hyper::client::conn::http1::SendRequest<tangram_util::http::Outgoing>> {
		Ok(match &self.inner.address {
			Address::Unix(path) => self.connect_unix_h1(path).await?,
			Address::Inet(inet) if self.inner.tls => self.connect_tcp_tls_h1(inet).await?,
			Address::Inet(inet) => self.connect_tcp_h1(inet).await?,
		})
	}

	async fn connect_h2(
		&self,
	) -> Result<hyper::client::conn::http2::SendRequest<tangram_util::http::Outgoing>> {
		Ok(match &self.inner.address {
			Address::Unix(path) => self.connect_unix_h2(path).await?,
			Address::Inet(inet) if self.inner.tls => self.connect_tcp_tls_h2(inet).await?,
			Address::Inet(inet) => self.connect_tcp_h2(inet).await?,
		})
	}

	async fn connect_unix_h1(
		&self,
		path: &std::path::Path,
	) -> Result<hyper::client::conn::http1::SendRequest<tangram_util::http::Outgoing>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.wrap_err("Failed to connect to the socket.")?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "The connection failed.");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		Ok(sender)
	}

	async fn connect_unix_h2(
		&self,
		path: &std::path::Path,
	) -> Result<hyper::client::conn::http2::SendRequest<tangram_util::http::Outgoing>> {
		// Connect via UNIX.
		let stream = UnixStream::connect(path)
			.await
			.wrap_err("Failed to connect to the socket.")?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "The connection failed.");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		Ok(sender)
	}

	async fn connect_tcp_h1(
		&self,
		inet: &Inet,
	) -> Result<hyper::client::conn::http1::SendRequest<tangram_util::http::Outgoing>> {
		// Connect via TCP.
		let stream = TcpStream::connect(inet.to_string())
			.await
			.wrap_err("Failed to create the TCP connection.")?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "The connection failed.");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		Ok(sender)
	}

	async fn connect_tcp_h2(
		&self,
		inet: &Inet,
	) -> Result<hyper::client::conn::http2::SendRequest<tangram_util::http::Outgoing>> {
		// Connect via TCP.
		let stream = TcpStream::connect(inet.to_string())
			.await
			.wrap_err("Failed to create the TCP connection.")?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "The connection failed.");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		Ok(sender)
	}

	async fn connect_tcp_tls_h1(
		&self,
		inet: &Inet,
	) -> Result<hyper::client::conn::http1::SendRequest<tangram_util::http::Outgoing>> {
		// Connect via TLS over TCP.
		let stream = self.connect_tcp_tls(inet).await?;

		// Perform the HTTP handshake.
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.with_upgrades()
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "The connection failed.");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		Ok(sender)
	}

	async fn connect_tcp_tls_h2(
		&self,
		inet: &Inet,
	) -> Result<hyper::client::conn::http2::SendRequest<tangram_util::http::Outgoing>> {
		// Connect via TLS over TCP.
		let stream = self.connect_tcp_tls(inet).await?;

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			connection
				.await
				.inspect_err(|error| {
					tracing::error!(error = ?error, "The connection failed.");
				})
				.ok();
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		Ok(sender)
	}

	async fn connect_tcp_tls(
		&self,
		inet: &Inet,
	) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
		// Connect via TCP.
		let stream = TcpStream::connect(inet.to_string())
			.await
			.wrap_err("Failed to create the TCP connection.")?;

		// Create the connector.
		let mut root_store = rustls::RootCertStore::empty();
		root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
		let mut config = rustls::ClientConfig::builder()
			.with_root_certificates(root_store)
			.with_no_client_auth();
		config.alpn_protocols = vec!["h2".into()];
		let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

		// Create the server name.
		let server_name = rustls_pki_types::ServerName::try_from(inet.host.to_string().as_str())
			.wrap_err("Failed to create the server name.")?
			.to_owned();

		// Connect via TLS.
		let stream = connector
			.connect(server_name, stream)
			.await
			.wrap_err("Failed to connect.")?;

		// Verify the negotiated protocol.
		if !stream
			.get_ref()
			.1
			.alpn_protocol()
			.map_or(false, |protocol| protocol == b"h2")
		{
			return Err(error!("Failed to negotiate the HTTP/2 protocol."));
		}

		Ok(stream)
	}

	async fn send(
		&self,
		request: http::request::Request<tangram_util::http::Outgoing>,
	) -> Result<http::Response<tangram_util::http::Incoming>> {
		self.sender()
			.await?
			.send_request(request)
			.await
			.wrap_err("Failed to send the request.")
	}
}

impl Builder {
	#[must_use]
	pub fn new(address: Address) -> Self {
		Self {
			address,
			build: None,
			tls: None,
			user: None,
		}
	}

	#[must_use]
	pub fn build_(mut self, build: build::Id) -> Self {
		self.build = Some(build);
		self
	}

	#[must_use]
	pub fn tls(mut self, tls: bool) -> Self {
		self.tls = Some(tls);
		self
	}

	#[must_use]
	pub fn user(mut self, user: Option<User>) -> Self {
		self.user = user;
		self
	}

	#[must_use]
	pub fn build(self) -> Client {
		let address = self.address;
		let build = self.build;
		let file_descriptor_semaphore = tokio::sync::Semaphore::new(16);
		let sender = tokio::sync::Mutex::new(None);
		let tls = self.tls.unwrap_or(false);
		let user = self.user;
		let inner = Arc::new(Inner {
			address,
			build,
			file_descriptor_semaphore,
			sender,
			tls,
			user,
		});
		Client { inner }
	}
}

#[async_trait]
impl Handle for Client {
	fn clone_box(&self) -> Box<dyn Handle> {
		Box::new(self.clone())
	}

	async fn path(&self) -> Result<Option<tg::Path>> {
		self.path().await
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		&self.inner.file_descriptor_semaphore
	}

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		self.check_in_artifact(arg).await
	}

	async fn check_out_artifact(&self, arg: tg::artifact::CheckOutArg) -> Result<()> {
		self.check_out_artifact(arg).await
	}

	async fn list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		self.list_builds(arg).await
	}

	async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		self.try_get_build(id).await
	}

	async fn put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<()> {
		self.put_build(user, id, arg).await
	}

	async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> Result<()> {
		self.push_build(user, id).await
	}

	async fn pull_build(&self, id: &tg::build::Id) -> Result<()> {
		self.pull_build(id).await
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		self.get_or_create_build(user, arg).await
	}

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		self.try_get_build_status(id, arg, stop).await
	}

	async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		self.set_build_status(user, id, status).await
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::children::Chunk>>>> {
		self.try_get_build_children(id, arg, stop).await
	}

	async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		self.add_build_child(user, build_id, child_id).await
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		self.try_get_build_log(id, arg, stop).await
	}

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		self.add_build_log(user, id, bytes).await
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		self.try_get_build_outcome(id, arg, stop).await
	}

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		self.set_build_outcome(user, id, outcome).await
	}

	async fn format(&self, text: String) -> Result<String> {
		self.format(text).await
	}

	async fn lsp(
		&self,
		input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> Result<()> {
		self.lsp(input, output).await
	}

	async fn try_get_object(&self, id: &tg::object::Id) -> Result<Option<tg::object::GetOutput>> {
		self.try_get_object(id).await
	}

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput> {
		self.put_object(id, arg).await
	}

	async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		self.push_object(id).await
	}

	async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		self.pull_object(id).await
	}

	async fn search_packages(
		&self,
		arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		self.search_packages(arg).await
	}

	async fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		self.try_get_package(dependency, arg).await
	}

	async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		self.try_get_package_versions(dependency).await
	}

	async fn publish_package(&self, user: Option<&tg::User>, id: &tg::directory::Id) -> Result<()> {
		self.publish_package(user, id).await
	}

	async fn check_package(&self, dependency: &tg::Dependency) -> Result<Vec<tg::Diagnostic>> {
		self.check_package(dependency).await
	}

	async fn format_package(&self, dependency: &tg::Dependency) -> Result<()> {
		self.format_package(dependency).await
	}

	async fn get_runtime_doc(&self) -> Result<serde_json::Value> {
		self.get_runtime_doc().await
	}

	async fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
	) -> Result<Option<serde_json::Value>> {
		self.try_get_package_doc(dependency).await
	}

	async fn health(&self) -> Result<tg::Health> {
		self.health().await
	}

	async fn clean(&self) -> Result<()> {
		self.clean().await
	}

	async fn stop(&self) -> Result<()> {
		self.stop().await
	}

	async fn create_login(&self) -> Result<tg::user::Login> {
		self.create_login().await
	}

	async fn get_login(&self, id: &tg::Id) -> Result<Option<tg::Login>> {
		self.get_login(id).await
	}

	async fn get_user_for_token(&self, token: &str) -> Result<Option<tg::User>> {
		self.get_user_for_token(token).await
	}

	async fn create_oauth_url(&self, _id: &tg::Id) -> Result<Url> {
		Err(error!("unimplemented"))
	}

	async fn complete_login(&self, _id: &tg::Id, _code: String) -> Result<()> {
		Err(error!("unimplemented"))
	}
}

impl Address {
	#[must_use]
	pub fn is_local(&self) -> bool {
		match &self {
			Address::Unix(_) => true,
			Address::Inet(inet) => match &inet.host {
				Host::Domain(domain) => domain == "localhost",
				Host::Ip(ip) => ip.is_loopback(),
			},
		}
	}
}

impl std::fmt::Display for Address {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Address::Unix(path) => write!(f, "unix:{}", path.display()),
			Address::Inet(inet) => write!(f, "{inet}"),
		}
	}
}

impl std::fmt::Display for Inet {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}:{}", self.host, self.port)
	}
}

impl std::fmt::Display for Host {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Host::Ip(ip) => write!(f, "{ip}"),
			Host::Domain(domain) => write!(f, "{domain}"),
		}
	}
}

impl std::str::FromStr for Address {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (host, port) = s
			.split_once(':')
			.map_or((s, None), |(host, port)| (host, Some(port)));
		let host = host.parse().wrap_err("Failed to parse the host.")?;
		if matches!(&host, Host::Domain(hostname) if hostname == "unix") {
			let path = port.wrap_err("Expected a path.")?;
			Ok(Address::Unix(path.into()))
		} else {
			let port = port
				.wrap_err("Expected a port.")?
				.parse()
				.wrap_err("Failed to parse the port.")?;
			Ok(Address::Inet(Inet { host, port }))
		}
	}
}

impl std::str::FromStr for Host {
	type Err = std::net::AddrParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(ip) = s.parse() {
			Ok(Host::Ip(ip))
		} else {
			Ok(Host::Domain(s.to_string()))
		}
	}
}

impl TryFrom<Url> for Address {
	type Error = Error;

	fn try_from(value: Url) -> Result<Self, Self::Error> {
		let host = value
			.host_str()
			.wrap_err("Invalid URL.")?
			.parse()
			.wrap_err("Invalid URL.")?;
		let port = value.port_or_known_default().wrap_err("Invalid URL.")?;
		Ok(Address::Inet(Inet { host, port }))
	}
}
