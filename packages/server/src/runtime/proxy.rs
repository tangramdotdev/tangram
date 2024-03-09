use crate::Http;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use url::Url;

#[derive(Clone)]
pub struct Proxy {
	inner: Arc<Inner>,
}

pub struct Inner {
	build: tg::build::Id,
	http: std::sync::Mutex<Option<Http>>,
	server: crate::Server,
	shutdown: tokio::sync::watch::Sender<bool>,
	shutdown_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOrCreateProxiedArg {
	pub remote: bool,
	pub retry: tg::build::Retry,
	pub target: tg::target::Id,
}

impl Proxy {
	pub async fn start(
		server: &crate::Server,
		build: &tg::build::Id,
		address: tg::Address,
	) -> Result<Self> {
		// Create an owned copy of the parent build id to be used in the proxy server.
		let build = build.clone();

		// Set up a mutex to store an HTTP server.
		let http = std::sync::Mutex::new(None);

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(false);

		// Create the shutdown task.
		let shutdown_task = std::sync::Mutex::new(None);

		// Construct the proxy server.
		let inner = Inner {
			build,
			http,
			server: server.clone(),
			shutdown,
			shutdown_task,
		};
		let proxy = Proxy {
			inner: Arc::new(inner),
		};

		// Start the HTTP server.
		proxy
			.inner
			.http
			.lock()
			.unwrap()
			.replace(Http::start(&proxy, address));

		Ok(proxy)
	}

	pub fn stop(&self) {
		let server = self.clone();
		let task = tokio::spawn(async move {
			// Stop the http server.
			if let Some(http) = server.inner.http.lock().unwrap().as_ref() {
				http.stop();
			}

			// Join the http server.
			let http = server.inner.http.lock().unwrap().take();
			if let Some(http) = http {
				http.join().await?;
			}

			Ok::<_, tangram_error::Error>(())
		});
		self.inner.shutdown_task.lock().unwrap().replace(task);
		self.inner.shutdown.send_replace(true);
	}

	pub async fn join(&self) -> Result<()> {
		self.inner
			.shutdown
			.subscribe()
			.wait_for(|shutdown| *shutdown)
			.await
			.unwrap();
		let task = self.inner.shutdown_task.lock().unwrap().take();
		if let Some(task) = task {
			task.await.unwrap()?;
		}
		Ok(())
	}

	pub async fn get_or_create_build_proxied(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		if arg.parent.is_some() {
			return Err(error!("Using proxied server, parent should be set to None"));
		}
		let arg = tg::build::GetOrCreateArg {
			parent: Some(self.inner.build.clone()),
			remote: arg.remote,
			retry: arg.retry,
			target: arg.target,
		};
		self.inner.server.get_or_create_build(user, arg).await
	}
}

#[async_trait]
impl tg::Handle for Proxy {
	fn clone_box(&self) -> Box<dyn tg::Handle> {
		Box::new(self.clone())
	}

	async fn path(&self) -> Result<Option<tg::Path>> {
		self.inner.server.path().await
	}

	async fn format(&self, _text: String) -> Result<String> {
		Err(error!("Not supported"))
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		self.inner.server.file_descriptor_semaphore()
	}

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> Result<tg::artifact::CheckInOutput> {
		self.inner.server.check_in_artifact(arg).await
	}

	async fn check_out_artifact(&self, arg: tg::artifact::CheckOutArg) -> Result<()> {
		self.inner.server.check_out_artifact(arg).await
	}

	async fn list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		self.inner.server.list_builds(arg).await
	}

	async fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> Result<Option<tg::build::GetOutput>> {
		self.inner.server.try_get_build(id, arg).await
	}

	async fn put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> Result<()> {
		self.inner.server.put_build(user, id, arg).await
	}

	async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> Result<()> {
		self.inner.server.push_build(user, id).await
	}

	async fn pull_build(&self, id: &tg::build::Id) -> Result<()> {
		self.inner.server.pull_build(id).await
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		self.get_or_create_build_proxied(user, arg).await
	}

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		self.inner.server.try_get_build_status(id, arg, stop).await
	}

	async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		self.inner.server.set_build_status(user, id, status).await
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::children::Chunk>>>> {
		self.inner
			.server
			.try_get_build_children(id, arg, stop)
			.await
	}

	async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		self.inner
			.server
			.add_build_child(user, build_id, child_id)
			.await
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::log::Chunk>>>> {
		self.inner.server.try_get_build_log(id, arg, stop).await
	}

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		self.inner.server.add_build_log(user, build_id, bytes).await
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<Option<tg::build::Outcome>>> {
		self.inner.server.try_get_build_outcome(id, arg, stop).await
	}

	async fn set_build_outcome(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		self.inner.server.set_build_outcome(user, id, outcome).await
	}

	async fn try_get_object(&self, id: &tg::object::Id) -> Result<Option<tg::object::GetOutput>> {
		self.inner.server.try_get_object(id).await
	}

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> Result<tg::object::PutOutput> {
		self.inner.server.put_object(id, arg).await
	}

	async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		self.inner.server.push_object(id).await
	}

	async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		self.inner.server.pull_object(id).await
	}

	async fn search_packages(
		&self,
		_arg: tg::package::SearchArg,
	) -> Result<tg::package::SearchOutput> {
		Err(error!("Not supported"))
	}

	async fn try_get_package(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::GetArg,
	) -> Result<Option<tg::package::GetOutput>> {
		Err(error!("Not supported"))
	}

	async fn try_get_package_versions(
		&self,
		_dependency: &tg::Dependency,
	) -> Result<Option<Vec<String>>> {
		Err(error!("Not supported"))
	}

	async fn publish_package(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::directory::Id,
	) -> Result<()> {
		Err(error!("Not supported"))
	}

	async fn check_package(&self, _dependency: &tg::Dependency) -> Result<Vec<tg::Diagnostic>> {
		Err(error!("Not supported"))
	}

	async fn format_package(&self, _dependency: &tg::Dependency) -> Result<()> {
		Err(error!("Not supported"))
	}

	async fn get_runtime_doc(&self) -> Result<serde_json::Value> {
		Err(error!("Not supported"))
	}

	async fn try_get_package_doc(
		&self,
		_dependency: &tg::Dependency,
	) -> Result<Option<serde_json::Value>> {
		Err(error!("Not supported"))
	}

	async fn lsp(
		&self,
		_input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		_output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> Result<()> {
		Err(error!("Not supported"))
	}

	async fn health(&self) -> Result<tg::server::Health> {
		self.inner.server.health().await
	}

	async fn clean(&self) -> Result<()> {
		Err(error!("Not supported"))
	}

	async fn stop(&self) -> Result<()> {
		Err(error!("Not supported"))
	}

	async fn create_login(&self) -> Result<tg::user::Login> {
		Err(error!("Not supported"))
	}

	async fn get_login(&self, _id: &tg::Id) -> Result<Option<tg::user::Login>> {
		Err(error!("Not supported"))
	}

	async fn get_user_for_token(&self, _token: &str) -> Result<Option<tg::user::User>> {
		Err(error!("Not supported"))
	}

	async fn create_oauth_url(&self, _id: &tg::Id) -> Result<Url> {
		Err(error!("Not supported"))
	}

	async fn complete_login(&self, _id: &tg::Id, _code: String) -> Result<()> {
		Err(error!("Not supported"))
	}
}
