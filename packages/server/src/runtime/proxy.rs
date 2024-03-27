use crate::Http;
use bytes::Bytes;
use futures::Stream;
use std::sync::Arc;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncWrite};
use url::Url;

#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

pub struct Inner {
	build: tg::build::Id,
	http: std::sync::Mutex<Option<Http<Server>>>,
	path_map: Option<PathMap>,
	server: crate::Server,
	shutdown: tokio::sync::watch::Sender<bool>,
	shutdown_task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PathMap {
	pub output_guest: tg::Path,
	pub output_host: tg::Path,
	pub root_host: tg::Path,
}

impl Server {
	pub async fn start(
		server: &crate::Server,
		build: &tg::build::Id,
		url: Url,
		path_map: Option<PathMap>,
	) -> tg::Result<Self> {
		// Create an owned copy of the parent build id to be used in the proxy server.
		let build = build.clone();

		// Set up a mutex to store an HTTP server.
		let http = std::sync::Mutex::new(None);

		// Create the shutdown channel.
		let (shutdown, _) = tokio::sync::watch::channel(false);

		// Create the shutdown task.
		let shutdown_task = std::sync::Mutex::new(None);

		// Create the server.
		let inner = Inner {
			build,
			http,
			path_map,
			server: server.clone(),
			shutdown,
			shutdown_task,
		};
		let server = Server {
			inner: Arc::new(inner),
		};

		// Start the HTTP server.
		let http = Http::start(&server, url);
		server.inner.http.lock().unwrap().replace(http);

		Ok(server)
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

			Ok::<_, tg::Error>(())
		});
		self.inner.shutdown_task.lock().unwrap().replace(task);
		self.inner.shutdown.send_replace(true);
	}

	pub async fn join(&self) -> tg::Result<()> {
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

	fn host_path_for_guest_path(&self, path: tg::Path) -> tg::Result<tg::Path> {
		// Get the path map. If there is no path map, then the guest path is the host path.
		let Some(path_map) = &self.inner.path_map else {
			return Ok(path);
		};

		// Map the path.
		if let Some(path) = path.strip_prefix(&path_map.output_guest) {
			Ok(path_map.output_host.clone().join(path))
		} else {
			let path = path
				.strip_prefix(&"/".parse().unwrap())
				.ok_or_else(|| tg::error!("the path must be absolute"))?;
			Ok(path_map.root_host.clone().join(path))
		}
	}
}

impl tg::Handle for Server {
	async fn path(&self) -> tg::Result<Option<tg::Path>> {
		self.inner.server.path().await
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		self.inner.server.file_descriptor_semaphore()
	}

	async fn check_in_artifact(
		&self,
		mut arg: tg::artifact::CheckInArg,
	) -> tg::Result<tg::artifact::CheckInOutput> {
		// Replace the path with the host path.
		arg.path = self.host_path_for_guest_path(arg.path)?;

		// Perform the checkin.
		let output = self.inner.server.check_in_artifact(arg).await?;

		// If the VFS is disabled, then check out the artifact.
		if !self.inner.server.inner.options.vfs.enable {
			let arg = tg::artifact::CheckOutArg::default();
			self.check_out_artifact(&output.id, arg).await?;
		}

		Ok(output)
	}

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		mut arg: tg::artifact::CheckOutArg,
	) -> tg::Result<tg::artifact::CheckOutOutput> {
		// Replace the path with the host path.
		if let Some(path) = &mut arg.path {
			*path = self.host_path_for_guest_path(path.clone())?;
		}

		// Perform the checkout.
		self.inner.server.check_out_artifact(id, arg).await
	}

	async fn list_builds(&self, _arg: tg::build::ListArg) -> tg::Result<tg::build::ListOutput> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> tg::Result<Option<tg::build::GetOutput>> {
		self.inner.server.try_get_build(id, arg).await
	}

	async fn put_build(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::build::Id,
		_arg: &tg::build::PutArg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn push_build(&self, _user: Option<&tg::User>, _id: &tg::build::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn pull_build(&self, _id: &tg::build::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		mut arg: tg::build::GetOrCreateArg,
	) -> tg::Result<tg::build::GetOrCreateOutput> {
		arg.parent = Some(self.inner.build.clone());
		self.inner.server.get_or_create_build(user, arg).await
	}

	async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>> {
		self.inner.server.try_get_build_status(id, arg, stop).await
	}

	async fn set_build_status(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::build::Id,
		_status: tg::build::Status,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Chunk>> + Send + 'static>,
	> {
		self.inner
			.server
			.try_get_build_children(id, arg, stop)
			.await
	}

	async fn add_build_child(
		&self,
		_user: Option<&tg::User>,
		_build_id: &tg::build::Id,
		_child_id: &tg::build::Id,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		self.inner.server.try_get_build_log(id, arg, stop).await
	}

	async fn add_build_log(
		&self,
		_user: Option<&tg::User>,
		_build_id: &tg::build::Id,
		_bytes: Bytes,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		self.inner.server.try_get_build_outcome(id, arg, stop).await
	}

	async fn set_build_outcome(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::build::Id,
		_outcome: tg::build::Outcome,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::GetOutput>> {
		self.inner.server.try_get_object(id).await
	}

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: &tg::object::PutArg,
	) -> tg::Result<tg::object::PutOutput> {
		self.inner.server.put_object(id, arg).await
	}

	async fn push_object(&self, _id: &tg::object::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn pull_object(&self, _id: &tg::object::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn search_packages(
		&self,
		_arg: tg::package::SearchArg,
	) -> tg::Result<tg::package::SearchOutput> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::GetArg,
	) -> tg::Result<Option<tg::package::GetOutput>> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package_versions(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<Option<Vec<String>>> {
		Err(tg::error!("forbidden"))
	}

	async fn publish_package(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::directory::Id,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn yank_package(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::directory::Id,
	) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn check_package(&self, _dependency: &tg::Dependency) -> tg::Result<Vec<tg::Diagnostic>> {
		Err(tg::error!("forbidden"))
	}

	async fn format_package(&self, _dependency: &tg::Dependency) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn get_package_outdated(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<tg::package::OutdatedOutput> {
		Err(tg::error!("forbidden"))
	}

	async fn get_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package_doc(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		Err(tg::error!("forbidden"))
	}

	async fn format(&self, _text: String) -> tg::Result<String> {
		Err(tg::error!("forbidden"))
	}

	async fn lsp(
		&self,
		_input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		_output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn health(&self) -> tg::Result<tg::server::Health> {
		Err(tg::error!("forbidden"))
	}

	async fn clean(&self) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn stop(&self) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn create_login(&self) -> tg::Result<tg::user::Login> {
		Err(tg::error!("forbidden"))
	}

	async fn get_login(&self, _id: &tg::Id) -> tg::Result<Option<tg::user::Login>> {
		Err(tg::error!("forbidden"))
	}

	async fn get_user_for_token(&self, _token: &str) -> tg::Result<Option<tg::user::User>> {
		Err(tg::error!("forbidden"))
	}

	async fn list_watches(&self) -> tg::Result<Vec<tg::Path>> {
		Err(tg::error!("forbidden"))
	}

	async fn remove_watch(&self, _path: &tg::Path) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}
}
