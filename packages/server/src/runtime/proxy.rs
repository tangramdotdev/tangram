use crate::Http;
use bytes::Bytes;
use derive_more::FromStr;
use futures::Stream;
use itertools::Itertools;
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
pub struct GetOrCreateProxiedArg {
	pub remote: bool,
	pub retry: tg::build::Retry,
	pub target: tg::target::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PathMap {
	pub output_host: tg::Path,
	pub output_guest: tg::Path,
	pub root_host: tg::Path,
	pub root_guest: tg::Path,
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

	pub async fn get_or_create_build_proxied(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> tg::Result<tg::build::GetOrCreateOutput> {
		if arg.parent.is_some() {
			return Err(tg::error!(
				"using proxied server, parent should be set to none"
			));
		}
		let arg = tg::build::GetOrCreateArg {
			parent: Some(self.inner.build.clone()),
			remote: arg.remote,
			retry: arg.retry,
			target: arg.target,
		};
		self.inner.server.get_or_create_build(user, arg).await
	}

	fn host_path_from_guest_path(&self, guest_path: tg::Path) -> tg::Result<tg::Path> {
		if self.inner.path_map.is_none() {
			return Ok(guest_path);
		}
		let path_map = self.inner.path_map.as_ref().unwrap();
		let guest_path_components = guest_path.clone().into_components();
		let guest_output_components = path_map.output_guest.components();
		let guest_root_components = path_map.root_guest.components();

		let host_path = if guest_path_components.starts_with(guest_output_components) {
			let output_components = tg::Path::with_components(
				guest_path_components
					.into_iter()
					.skip(guest_output_components.len()),
			);
			path_map.output_host.clone().join(output_components)
		} else if guest_path_components.starts_with(guest_root_components) {
			let output_components = tg::Path::with_components(
				guest_path_components
					.into_iter()
					.skip(guest_root_components.len()),
			);
			path_map.root_host.clone().join(output_components)
		} else {
			return Err(tg::error!(%guest_path, "invalid path"));
		};

		Ok(host_path)
	}
}

impl tg::Handle for Server {
	async fn path(&self) -> tg::Result<Option<tg::Path>> {
		self.inner.server.path().await
	}

	async fn format(&self, _text: String) -> tg::Result<String> {
		Err(tg::error!("not supported"))
	}

	fn file_descriptor_semaphore(&self) -> &tokio::sync::Semaphore {
		self.inner.server.file_descriptor_semaphore()
	}

	async fn check_in_artifact(
		&self,
		arg: tg::artifact::CheckInArg,
	) -> tg::Result<tg::artifact::CheckInOutput> {
		// Remap incoming path.
		let host_path = self.host_path_from_guest_path(arg.path.clone())?;

		// Check if the path points into a Tangram artifact.
		let host_path_string = host_path.to_string();
		let output = if host_path_string.contains(".tangram/artifacts") {
			let parts = host_path_string.split("tangram/artifacts").collect_vec();
			if parts.len() != 2 {
				return Err(tg::error!(%host_path, "invalid artifact path"));
			}
			let artifact_subpath = tg::Path::from_str(parts[1])?;
			let artifact_subpath_components = artifact_subpath.into_components();
			if artifact_subpath_components.is_empty() {
				return Err(tg::error!(%host_path, "invalid artifact path"));
			}
			// Skip the root component, the next will be the artifact ID.
			let toplevel_artifact_id = &artifact_subpath_components[1];
			let toplevel_artifact_id = if let tg::path::Component::Normal(artifact_id) =
				toplevel_artifact_id
			{
				tg::artifact::Id::from_str(artifact_id)?
			} else {
				return Err(tg::error!(%host_path, "invalid artifact path, bat component type"));
			};

			// If there was no additional subpath, return the artifact ID.
			if artifact_subpath_components.len() == 1 {
				tg::artifact::CheckInOutput {
					id: toplevel_artifact_id,
				}
			} else {
				// If there was an additional subpath, locate the ID of the inner artifact.
				let toplevel_artifact = tg::Artifact::with_id(toplevel_artifact_id);
				let trailing_subpath =
					tg::Path::with_components(artifact_subpath_components[2..].iter().cloned())
						.to_string();
				let object = tg::symlink::Object {
					artifact: Some(toplevel_artifact),
					path: Some(trailing_subpath),
				};
				let symlink = tg::symlink::Symlink::with_object(object);
				let target = symlink
					.resolve(&self.inner.server)
					.await?
					.ok_or(tg::error!("could not resolve symlink"))?;
				let target_id = target.id(&self.inner.server).await?;
				tg::artifact::CheckInOutput { id: target_id }
			}
		} else {
			// Otherwise, perform the checkin.
			let arg = tg::artifact::CheckInArg { path: host_path };
			self.inner.server.check_in_artifact(arg).await?
		};

		// If the VFS is disabled, immediately perform an internal checkout of the newly checked-in artifact.
		if !self.inner.server.inner.options.vfs.enable {
			let arg = tg::artifact::CheckOutArg {
				path: None,
				..Default::default()
			};
			self.check_out_artifact(&output.id, arg).await?;
		}

		Ok(output)
	}

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::CheckOutArg,
	) -> tg::Result<tg::artifact::CheckOutOutput> {
		let arg = if let Some(path) = arg.path {
			let path = self.host_path_from_guest_path(path)?;
			tg::artifact::CheckOutArg {
				path: Some(path),
				..arg
			}
		} else {
			arg
		};
		self.inner.server.check_out_artifact(id, arg).await
	}

	async fn list_builds(&self, arg: tg::build::ListArg) -> tg::Result<tg::build::ListOutput> {
		self.inner.server.list_builds(arg).await
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
		user: Option<&tg::User>,
		id: &tg::build::Id,
		arg: &tg::build::PutArg,
	) -> tg::Result<()> {
		self.inner.server.put_build(user, id, arg).await
	}

	async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> tg::Result<()> {
		self.inner.server.push_build(user, id).await
	}

	async fn pull_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		self.inner.server.pull_build(id).await
	}

	async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> tg::Result<tg::build::GetOrCreateOutput> {
		self.get_or_create_build_proxied(user, arg).await
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
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> tg::Result<()> {
		self.inner.server.set_build_status(user, id, status).await
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
		user: Option<&tg::User>,
		_build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> tg::Result<()> {
		// Discard incoming build ID, use the one from the proxy server.
		let build_id = &self.inner.build;

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
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>>
	{
		self.inner.server.try_get_build_log(id, arg, stop).await
	}

	async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		bytes: Bytes,
	) -> tg::Result<()> {
		self.inner.server.add_build_log(user, build_id, bytes).await
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
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> tg::Result<()> {
		self.inner.server.set_build_outcome(user, id, outcome).await
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

	async fn push_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.inner.server.push_object(id).await
	}

	async fn pull_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		self.inner.server.pull_object(id).await
	}

	async fn search_packages(
		&self,
		_arg: tg::package::SearchArg,
	) -> tg::Result<tg::package::SearchOutput> {
		Err(tg::error!("not supported"))
	}

	async fn try_get_package(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::GetArg,
	) -> tg::Result<Option<tg::package::GetOutput>> {
		Err(tg::error!("not supported"))
	}

	async fn try_get_package_versions(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<Option<Vec<String>>> {
		Err(tg::error!("not supported"))
	}

	async fn publish_package(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::directory::Id,
	) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn yank_package(
		&self,
		_user: Option<&tg::User>,
		_id: &tg::directory::Id,
	) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn check_package(&self, _dependency: &tg::Dependency) -> tg::Result<Vec<tg::Diagnostic>> {
		Err(tg::error!("not supported"))
	}

	async fn format_package(&self, _dependency: &tg::Dependency) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn get_package_outdated(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<tg::package::OutdatedOutput> {
		Err(tg::error!("not supported"))
	}

	async fn get_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		Err(tg::error!("not supported"))
	}

	async fn try_get_package_doc(
		&self,
		_dependency: &tg::Dependency,
	) -> tg::Result<Option<serde_json::Value>> {
		Err(tg::error!("not supported"))
	}

	async fn lsp(
		&self,
		_input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		_output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn health(&self) -> tg::Result<tg::server::Health> {
		self.inner.server.health().await
	}

	async fn clean(&self) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn stop(&self) -> tg::Result<()> {
		Err(tg::error!("not supported"))
	}

	async fn create_login(&self) -> tg::Result<tg::user::Login> {
		Err(tg::error!("not supported"))
	}

	async fn get_login(&self, _id: &tg::Id) -> tg::Result<Option<tg::user::Login>> {
		Err(tg::error!("not supported"))
	}

	async fn get_user_for_token(&self, _token: &str) -> tg::Result<Option<tg::user::User>> {
		Err(tg::error!("not supported"))
	}
}
