use crate::{
	artifact, build, directory, lock, log, object, package, target, user, Dependency, Handle,
	Health, Id, Runtime, System, User,
};
use async_trait::async_trait;
use bytes::Bytes;
use derive_more::TryUnwrap;
use futures::{
	stream::{self, BoxStream},
	StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, BodyStream};
use itertools::Itertools;
use std::{path::PathBuf, sync::Arc};
use tangram_error::{return_error, Error, Result, Wrap, WrapErr};
use tokio::{
	io::{AsyncBufReadExt, AsyncReadExt},
	net::{TcpStream, UnixStream},
};
use tokio_util::io::StreamReader;
use url::Url;

#[derive(Debug, Clone)]
pub struct Client {
	inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
	addr: Addr,
	file_descriptor_semaphore: tokio::sync::Semaphore,
	sender: tokio::sync::Mutex<Option<hyper::client::conn::http2::SendRequest<Outgoing>>>,
	tls: bool,
	user: Option<User>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum Addr {
	Inet(Inet),
	Unix(PathBuf),
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
	addr: Addr,
	tls: Option<bool>,
	user: Option<User>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct CheckinArtifactBody {
	pub path: crate::Path,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct CheckoutArtifactBody {
	pub artifact: artifact::Id,
	pub path: crate::Path,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct SearchPackagesSearchParams {
	pub query: String,
}

#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct GetPackageSearchParams {
	pub lock: bool,
}

#[derive(serde::Deserialize, serde::Serialize, TryUnwrap)]
#[serde(untagged)]
#[try_unwrap(ref)]
pub enum GetPackageBody {
	Package(directory::Id),
	PackageAndLock((directory::Id, lock::Id)),
}

#[derive(Default, serde::Deserialize, serde::Serialize)]
pub struct GetBuildQueueItemSearchParams {
	#[serde(default)]
	pub hosts: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct GetOrCreateBuildSearchParams {
	#[serde(default)]
	pub parent: Option<build::Id>,
	#[serde(default)]
	pub depth: u64,
	#[serde(default)]
	pub remote: bool,
	#[serde(default)]
	pub retry: build::Retry,
	pub target: target::Id,
}

type Incoming = hyper::body::Incoming;

type Outgoing = http_body_util::combinators::UnsyncBoxBody<
	::bytes::Bytes,
	Box<dyn std::error::Error + Send + Sync + 'static>,
>;

impl Client {
	fn new(addr: Addr, tls: bool, user: Option<User>) -> Self {
		let file_descriptor_semaphore = tokio::sync::Semaphore::new(16);
		let sender = tokio::sync::Mutex::new(None);
		let inner = Arc::new(Inner {
			addr,
			file_descriptor_semaphore,
			sender,
			tls,
			user,
		});
		Self { inner }
	}

	pub async fn connect(&self) -> Result<()> {
		self.sender().await.map(|_| ())
	}

	pub async fn disconnect(&self) -> Result<()> {
		self.inner.sender.lock().await.take();
		Ok(())
	}

	async fn sender(&self) -> Result<hyper::client::conn::http2::SendRequest<Outgoing>> {
		if let Some(sender) = self.inner.sender.lock().await.as_ref().cloned() {
			if sender.is_ready() {
				return Ok(sender);
			}
		}
		match &self.inner.addr {
			Addr::Inet(inet) if self.inner.tls => {
				self.connect_tcp_tls(inet).await?;
			},
			Addr::Inet(inet) => {
				self.connect_tcp(inet).await?;
			},
			Addr::Unix(path) => {
				self.connect_unix(path).await?;
			},
		}
		Ok(self.inner.sender.lock().await.as_ref().cloned().unwrap())
	}

	async fn connect_tcp(&self, inet: &Inet) -> Result<()> {
		let mut sender_guard = self.inner.sender.lock().await;

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
			if let Err(error) = connection.await {
				tracing::error!(error = ?error, "The connection failed.");
			}
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		// Replace the sender.
		sender_guard.replace(sender);

		Ok(())
	}

	async fn connect_tcp_tls(&self, inet: &Inet) -> Result<()> {
		let mut sender_guard = self.inner.sender.lock().await;

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
			.map(|protocol| protocol == b"h2")
			.unwrap_or_default()
		{
			return_error!("Failed to negotiate the HTTP/2 protocol.");
		}

		// Perform the HTTP handshake.
		let executor = hyper_util::rt::TokioExecutor::new();
		let io = hyper_util::rt::TokioIo::new(stream);
		let (mut sender, connection) = hyper::client::conn::http2::handshake(executor, io)
			.await
			.wrap_err("Failed to perform the HTTP handshake.")?;

		// Spawn the connection.
		tokio::spawn(async move {
			if let Err(error) = connection.await {
				tracing::error!(error = ?error, "The connection failed.");
			}
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		// Replace the sender.
		sender_guard.replace(sender);

		Ok(())
	}

	async fn connect_unix(&self, path: &std::path::Path) -> Result<()> {
		let mut sender_guard = self.inner.sender.lock().await;

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
			if let Err(error) = connection.await {
				tracing::error!(error = ?error, "The connection failed.");
			}
		});

		// Wait for the sender to be ready.
		sender
			.ready()
			.await
			.wrap_err("Failed to ready the sender.")?;

		// Replace the sender.
		sender_guard.replace(sender);

		Ok(())
	}

	async fn send(
		&self,
		request: http::request::Request<Outgoing>,
	) -> Result<http::Response<Incoming>> {
		self.sender()
			.await?
			.send_request(request)
			.await
			.wrap_err("Failed to send the request.")
	}
}

#[async_trait]
impl Handle for Client {
	fn clone_box(&self) -> Box<dyn Handle> {
		Box::new(self.clone())
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		&self.inner.file_descriptor_semaphore
	}

	async fn health(&self) -> Result<Health> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri("/v1/health")
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let health = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(health)
	}

	async fn stop(&self) -> Result<()> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/v1/stop")
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		self.send(request).await.ok();
		Ok(())
	}

	async fn clean(&self) -> Result<()> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/v1/clean")
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn get_object_exists(&self, id: &object::Id) -> Result<bool> {
		let request = http::request::Builder::default()
			.method(http::Method::HEAD)
			.uri(format!("/v1/objects/{id}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(false);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(true)
	}

	async fn try_get_object(&self, id: &object::Id) -> Result<Option<Bytes>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/objects/{id}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		Ok(Some(bytes))
	}

	async fn try_put_object(&self, id: &object::Id, bytes: &Bytes) -> Result<object::PutOutput> {
		let body = full(bytes.clone());
		let request = http::request::Builder::default()
			.method(http::Method::PUT)
			.uri(format!("/v1/objects/{id}"))
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let output = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		return Ok(output);
	}

	async fn push_object(&self, id: &object::Id) -> Result<()> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(format!("/v1/objects/{id}/push"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn pull_object(&self, id: &object::Id) -> Result<()> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(format!("/v1/objects/{id}/pull"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn check_in_artifact(&self, path: &crate::Path) -> Result<artifact::Id> {
		let body = CheckinArtifactBody { path: path.clone() };
		let body = serde_json::to_string(&body).wrap_err("Failed to serialize the body.")?;
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/v1/artifacts/checkin")
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let id = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(id)
	}

	async fn check_out_artifact(&self, id: &artifact::Id, path: &crate::Path) -> Result<()> {
		let body = CheckoutArtifactBody {
			artifact: id.clone(),
			path: path.clone(),
		};
		let body = serde_json::to_string(&body).wrap_err("Failed to serialize the body.")?;
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/v1/artifacts/checkout")
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn try_get_assignment(&self, id: &target::Id) -> Result<Option<build::Id>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/assignments/{id}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let id = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(Some(id))
	}

	async fn get_build_exists(&self, id: &build::Id) -> Result<bool> {
		let request = http::request::Builder::default()
			.method(http::Method::HEAD)
			.uri(format!("/v1/builds/{id}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(false);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(true)
	}

	async fn try_get_build(&self, id: &build::Id) -> Result<Option<build::Data>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/builds/{id}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let data = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(data)
	}

	async fn try_put_build(
		&self,
		user: Option<&User>,
		id: &build::Id,
		data: &build::Data,
	) -> Result<build::PutOutput> {
		let mut request = http::request::Builder::default()
			.method(http::Method::PUT)
			.uri(format!("/v1/builds/{id}"));
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let json = serde_json::to_string(&data).wrap_err("Failed to serialize the data.")?;
		let body = full(json);
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let output = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		return Ok(output);
	}

	async fn get_or_create_build(
		&self,
		target: &target::Id,
		options: build::Options,
	) -> Result<build::Id> {
		let parent = options.parent.map(|parent| parent.id().clone());
		let search_params = GetOrCreateBuildSearchParams {
			parent,
			depth: options.depth,
			remote: options.remote,
			retry: options.retry,
			target: target.clone(),
		};
		let search_params = serde_urlencoded::to_string(search_params)
			.wrap_err("Failed to serialize the search params.")?;
		let uri = format!("/v1/builds?{search_params}");
		let mut request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(uri);
		let user = options.user.as_ref().or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let request = request
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let id = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(id)
	}

	async fn try_get_queue_item(
		&self,
		user: Option<&User>,
		hosts: Option<Vec<System>>,
	) -> Result<Option<build::queue::Item>> {
		let mut uri = "/v1/queue".to_owned();
		let hosts = hosts.map(|hosts| hosts.iter().map(ToString::to_string).join(","));
		let search_params = GetBuildQueueItemSearchParams { hosts };
		let search_params = serde_urlencoded::to_string(&search_params)
			.wrap_err("Failed to serialize the search params.")?;
		uri.push_str(&format!("?{search_params}"));
		let mut request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let request = request
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let item =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(item)
	}

	async fn try_get_build_status(&self, id: &build::Id) -> Result<Option<build::Status>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/builds/{id}/status"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let status = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(status)
	}

	async fn set_build_status(
		&self,
		user: Option<&User>,
		id: &build::Id,
		status: build::Status,
	) -> Result<()> {
		let mut request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(format!("/v1/builds/{id}/status"));
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&status).wrap_err("Failed to serialize the body.")?;
		let request = request
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn try_get_build_target(&self, id: &build::Id) -> Result<Option<target::Id>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/builds/{id}/target"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let id = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(id)
	}

	async fn try_get_build_children(
		&self,
		id: &build::Id,
	) -> Result<Option<BoxStream<'static, Result<build::Id>>>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/builds/{id}/children"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let stream = BodyStream::new(response.into_body())
			.filter_map(|frame| async {
				match frame.map(http_body::Frame::into_data) {
					Ok(Ok(bytes)) => Some(Ok(bytes)),
					Err(e) => Some(Err(e)),
					Ok(Err(_frame)) => None,
				}
			})
			.map_err(|error| error.wrap("Failed to read from the body."));
		let reader = Box::pin(tokio::io::BufReader::new(StreamReader::new(
			stream.map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error)),
		)));
		let children = stream::try_unfold((true, reader), move |(first, mut reader)| async move {
			if first {
				let byte = reader
					.read_u8()
					.await
					.wrap_err("Failed to read from the reader.")?;
				if byte != b'[' {
					return_error!("Expected an open bracket.");
				}
			}
			let mut byte = reader
				.read_u8()
				.await
				.wrap_err("Failed to read from the reader.")?;
			if byte == b']' {
				return Ok(None);
			}
			if !first {
				if byte != b',' {
					return_error!("Expected a comma.");
				}
				byte = reader
					.read_u8()
					.await
					.wrap_err("Failed to read from the reader.")?;
			}
			if byte != b'"' {
				return_error!("Expected a quotation mark.");
			}
			let mut id = Vec::new();
			reader
				.read_until(b'"', &mut id)
				.await
				.wrap_err("Failed to read from the reader.")?;
			id.pop();
			let id = String::from_utf8(id).wrap_err("Failed to deserialize the ID.")?;
			let id = id.parse().wrap_err("Failed to deserialize the ID.")?;
			Ok(Some((id, (false, reader))))
		});
		Ok(Some(children.boxed()))
	}

	async fn add_build_child(
		&self,
		user: Option<&User>,
		build_id: &build::Id,
		child_id: &build::Id,
	) -> Result<()> {
		let mut request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(format!("/v1/builds/{build_id}/children"));
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&child_id).wrap_err("Failed to serialize the body.")?;
		let request = request
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn try_get_build_log(
		&self,
		id: &build::Id,
		pos: Option<u64>,
		len: Option<i64>,
	) -> Result<Option<BoxStream<'static, Result<log::Entry>>>> {
		let params = serde_urlencoded::to_string(log::Params { pos, len }).unwrap();
		let uri = if params.is_empty() {
			format!("/v1/builds/{id}/log")
		} else {
			format!("/v1/builds/{id}/log?{params}")
		};
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(uri)
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self
			.send(request)
			.await
			.wrap_err("Failed to send the request.")?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let stream = BodyStream::new(response.into_body())
			.filter_map(|frame| async {
				match frame.map(http_body::Frame::into_data) {
					Ok(Ok(bytes)) => Some(Ok(bytes)),
					Err(e) => Some(Err(e)),
					Ok(Err(_frame)) => None,
				}
			})
			.map_err(|error| error.wrap("Failed to read from the body."));
		let reader = Box::pin(tokio::io::BufReader::new(StreamReader::new(
			stream.map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error)),
		)));
		let log = stream::unfold(reader, |mut reader| async move {
			match log::Entry::read(&mut reader).await {
				Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => None,
				entry => Some((entry.wrap_err("Failed to read entry."), reader)),
			}
		});
		Ok(Some(log.boxed()))
	}

	async fn add_build_log(&self, user: Option<&User>, id: &build::Id, bytes: Bytes) -> Result<()> {
		let mut request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(format!("/v1/builds/{id}/log"));
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = bytes;
		let request = request
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn try_get_build_outcome(&self, id: &build::Id) -> Result<Option<build::Outcome>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/builds/{id}/outcome"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let outcome =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(Some(outcome))
	}

	async fn finish_build(
		&self,
		user: Option<&User>,
		id: &build::Id,
		outcome: build::Outcome,
	) -> Result<()> {
		let mut request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(format!("/v1/builds/{id}/finish"));
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let outcome = outcome.data(self).await?;
		let body = serde_json::to_vec(&outcome).wrap_err("Failed to serialize the body.")?;
		let request = request
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn search_packages(&self, query: &str) -> Result<Vec<String>> {
		let mut uri = "/v1/packages/search".to_owned();
		let search_params = SearchPackagesSearchParams {
			query: query.to_owned(),
		};
		let search_params = serde_urlencoded::to_string(search_params)
			.wrap_err("Failed to serialize the search params.")?;
		uri.push_str(&format!("?{search_params}"));
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(uri)
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}

	async fn try_get_package(&self, dependency: &Dependency) -> Result<Option<directory::Id>> {
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/packages/{dependency}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let body: Option<GetPackageBody> =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		let Some(body) = body else {
			return Ok(None);
		};
		let GetPackageBody::Package(package) = body else {
			return_error!("Unexpected body.");
		};
		Ok(Some(package))
	}

	async fn try_get_package_and_lock(
		&self,
		dependency: &Dependency,
	) -> Result<Option<(directory::Id, lock::Id)>> {
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let mut uri = format!("/v1/packages/{dependency}");
		let search_params = GetPackageSearchParams { lock: true };
		let search_params = serde_urlencoded::to_string(&search_params)
			.wrap_err("Failed to serialize the search params.")?;
		uri.push_str(&format!("?{search_params}"));
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(uri)
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let body: Option<GetPackageBody> =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		let Some(body) = body else {
			return Ok(None);
		};
		let GetPackageBody::PackageAndLock((package, lock)) = body else {
			return_error!("Unexpected body.");
		};
		Ok(Some((package, lock)))
	}

	async fn try_get_package_versions(
		&self,
		dependency: &Dependency,
	) -> Result<Option<Vec<String>>> {
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/packages/{dependency}/versions"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let id =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(Some(id))
	}

	async fn try_get_package_metadata(
		&self,
		dependency: &Dependency,
	) -> Result<Option<package::Metadata>> {
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/packages/{dependency}/metadata"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}

	async fn try_get_package_dependencies(
		&self,
		dependency: &Dependency,
	) -> Result<Option<Vec<Dependency>>> {
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/packages/{dependency}/dependencies"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}

	async fn publish_package(&self, user: Option<&User>, id: &directory::Id) -> Result<()> {
		let mut request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/v1/packages");
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&id).wrap_err("Failed to serialize the body.")?;
		let request = request
			.body(full(body))
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		Ok(())
	}

	async fn create_login(&self) -> Result<user::Login> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/v1/logins")
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}

	async fn get_login(&self, id: &Id) -> Result<Option<user::Login>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(format!("/v1/logins/{id}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self
			.send(request)
			.await
			.wrap_err("Failed to send the request.")?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}

	async fn get_user_for_token(&self, token: &str) -> Result<Option<user::User>> {
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri("/v1/user")
			.header(http::header::AUTHORIZATION, format!("Bearer {token}"))
			.body(empty())
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			return_error!("Expected the response's status to be success.");
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let response =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(response)
	}
}

impl Addr {
	#[must_use]
	pub fn is_local(&self) -> bool {
		match &self {
			Addr::Inet(inet) => match &inet.host {
				Host::Domain(domain) => domain == "localhost",
				Host::Ip(ip) => ip.is_loopback(),
			},
			Addr::Unix(_) => true,
		}
	}
}

impl std::fmt::Display for Addr {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Addr::Inet(inet) => write!(f, "{inet}"),
			Addr::Unix(path) => write!(f, "unix:{}", path.display()),
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

impl std::str::FromStr for Addr {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let mut parts = s.splitn(2, ':');
		let host = parts
			.next()
			.wrap_err("Expected a host.")?
			.parse()
			.wrap_err("Failed to parse the host.")?;
		if matches!(&host, Host::Domain(hostname) if hostname == "unix") {
			let path = parts.next().wrap_err("Expected a path.")?;
			Ok(Addr::Unix(path.into()))
		} else {
			let port = parts
				.next()
				.wrap_err("Expected a port.")?
				.parse()
				.wrap_err("Failed to parse the port.")?;
			Ok(Addr::Inet(Inet { host, port }))
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

impl TryFrom<Url> for Addr {
	type Error = Error;

	fn try_from(value: Url) -> Result<Self, Self::Error> {
		let host = value
			.host_str()
			.wrap_err("Invalid URL.")?
			.parse()
			.wrap_err("Invalid URL.")?;
		let port = value.port_or_known_default().wrap_err("Invalid URL.")?;
		Ok(Addr::Inet(Inet { host, port }))
	}
}
impl Builder {
	#[must_use]
	pub fn new(addr: Addr) -> Self {
		Self {
			addr,
			tls: None,
			user: None,
		}
	}

	#[must_use]
	pub fn with_runtime(runtime: Runtime) -> Self {
		Self::new(runtime.addr)
	}

	pub fn with_runtime_json(runtime_json: &impl AsRef<str>) -> Result<Self> {
		let runtime: Runtime = serde_json::from_str(runtime_json.as_ref())
			.wrap_err("Malformed TANGRAM_RUNTIME json string")?;
		Ok(Self::with_runtime(runtime))
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
		let tls = self.tls.unwrap_or(false);
		Client::new(self.addr, tls, self.user)
	}
}

#[must_use]
pub fn empty() -> Outgoing {
	http_body_util::Empty::new()
		.map_err(Into::into)
		.boxed_unsync()
}

#[must_use]
pub fn full(chunk: impl Into<::bytes::Bytes>) -> Outgoing {
	http_body_util::Full::new(chunk.into())
		.map_err(Into::into)
		.boxed_unsync()
}
