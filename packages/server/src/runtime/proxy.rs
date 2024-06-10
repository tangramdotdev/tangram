use crate::Server;
use futures::{future, stream, Future, Stream, StreamExt, TryStreamExt as _};
use std::{str::FromStr as _, sync::Arc};
use tangram_client as tg;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

#[derive(Clone)]
pub struct Proxy(Arc<Inner>);

pub struct Inner {
	build: tg::build::Id,
	path_map: Option<PathMap>,
	remote: Option<String>,
	server: Server,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PathMap {
	pub output_guest: tg::Path,
	pub output_host: tg::Path,
	pub root_host: tg::Path,
}

impl Proxy {
	pub fn new(
		server: crate::Server,
		build: tg::build::Id,
		remote: Option<String>,
		path_map: Option<PathMap>,
	) -> Self {
		let inner = Inner {
			build,
			path_map,
			remote,
			server,
		};
		Self(Arc::new(inner))
	}

	fn host_path_for_guest_path(&self, path: tg::Path) -> tg::Result<tg::Path> {
		// Get the path map. If there is no path map, then the guest path is the host path.
		let Some(path_map) = &self.path_map else {
			return Ok(path);
		};

		// Map the path.
		if let Some(path) = path
			.diff(&path_map.output_guest)
			.filter(tg::Path::is_internal)
		{
			Ok(path_map.output_host.clone().join(path))
		} else {
			let path = path
				.diff(&"/".parse().unwrap())
				.ok_or_else(|| tg::error!("the path must be absolute"))?;
			Ok(path_map.root_host.clone().join(path))
		}
	}

	fn guest_path_for_host_path(&self, path: tg::Path) -> tg::Result<tg::Path> {
		// Get the path map. If there is no path map, then the host path is the guest path.
		let Some(path_map) = &self.path_map else {
			return Ok(path);
		};

		// Map the path.
		let path = std::path::PathBuf::from(path.to_string());
		let result = if path.starts_with(&path_map.output_host) {
			let suffix: tg::Path = path
				.strip_prefix(&path_map.output_host)
				.unwrap()
				.to_owned()
				.try_into()?;
			path_map.output_guest.clone().join(suffix)
		} else if path.starts_with(&self.server.path) {
			let suffix: tg::Path = path
				.strip_prefix(&self.server.path)
				.unwrap()
				.to_owned()
				.try_into()?;
			tg::Path::from_str("/.tangram").unwrap().join(suffix)
		} else {
			let suffix: tg::Path = path
				.strip_prefix(&path_map.root_host)
				.map_err(|error| {
					tg::error!(source = error, "cannot map path outside of host root")
				})?
				.to_owned()
				.try_into()?;
			tg::Path::from_str("/").unwrap().join(suffix)
		};

		Ok(result)
	}
}

impl tg::Handle for Proxy {
	async fn check_in_artifact(
		&self,
		mut arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkin::Event>> + Send + 'static> {
		// Replace the path with the host path.
		arg.path = self.host_path_for_guest_path(arg.path)?;

		// Perform the checkin.
		let stream = self.server.check_in_artifact(arg).await?.and_then({
			let server = self.server.clone();
			move |event| {
				let server = server.clone();
				async move {
					if server.options.vfs.is_some() {
						return Ok(event);
					};
					let tg::artifact::checkin::Event::End(id) = &event else {
						return Ok(event);
					};
					tg::Artifact::with_id(id.clone())
						.check_out(&server, tg::artifact::checkout::Arg::default())
						.await?;
					Ok(event)
				}
			}
		});

		Ok(stream)
	}

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		mut arg: tg::artifact::checkout::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkout::Event>> + Send + 'static>
	{
		// Replace the path with the host path.
		if let Some(path) = &mut arg.path {
			*path = self.host_path_for_guest_path(path.clone())?;
		} else {
			// If there's no path set (internal checkout) and the VFS is enabled, ignore the request.
			if self.server.options.vfs.is_some() {
				let path = self
					.server
					.artifacts_path()
					.join(id.to_string())
					.try_into()?;
				let stream =
					stream::once(future::ready(Ok(tg::artifact::checkout::Event::End(path))))
						.left_stream();
				return Ok(stream);
			}
		}

		// Otherwise, remap the path.
		let proxy = self.clone();
		let stream = self
			.server
			.check_out_artifact(id, arg)
			.await?
			.and_then(move |event| {
				let proxy = proxy.clone();
				async move {
					let tg::artifact::checkout::Event::End(path) = event else {
						return Ok(event);
					};
					let path = proxy.guest_path_for_host_path(path)?;
					Ok(tg::artifact::checkout::Event::End(path))
				}
			})
			.right_stream();
		Ok(stream)
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> {
		self.server.create_blob(reader)
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
		self.server.try_read_blob_stream(id, arg)
	}

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> {
		self.server.try_get_build(id)
	}

	async fn put_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::put::Arg,
	) -> tg::Result<tg::build::put::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn push_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn pull_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::pull::Event>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn try_dequeue_build(
		&self,
		_arg: tg::build::dequeue::Arg,
	) -> tg::Result<Option<tg::build::dequeue::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn start_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::start::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>,
		>,
	> {
		self.server.try_get_build_status_stream(id)
	}

	fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		mut arg: tg::build::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::build::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		arg.remote = self.remote.clone();
		self.server.try_get_build_children_stream(id, arg)
	}

	async fn add_build_child(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::children::post::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		mut arg: tg::build::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::get::Event>> + Send + 'static>,
		>,
	> {
		arg.remote = self.remote.clone();
		self.server.try_get_build_log_stream(id, arg)
	}

	async fn add_build_log(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::log::post::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
		>,
	> {
		self.server.try_get_build_outcome_future(id)
	}

	async fn finish_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::finish::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn touch_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::touch::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn heartbeat_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::heartbeat::Arg,
	) -> tg::Result<tg::build::heartbeat::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn lsp(
		&self,
		_input: impl AsyncBufRead + Send + Unpin + 'static,
		_output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.server.try_get_object_metadata(id)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.server.try_get_object(id)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> {
		self.server.put_object(id, arg)
	}

	async fn push_object(
		&self,
		_id: &tg::object::Id,
		_arg: tg::object::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::push::Event>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn pull_object(
		&self,
		_id: &tg::object::Id,
		_arg: tg::object::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::pull::Event>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn list_packages(
		&self,
		_arg: tg::package::list::Arg,
	) -> tg::Result<tg::package::list::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::get::Arg,
	) -> tg::Result<Option<tg::package::get::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn check_package(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::check::Arg,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package_doc(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::doc::Arg,
	) -> tg::Result<Option<serde_json::Value>> {
		Err(tg::error!("forbidden"))
	}

	async fn format_package(&self, _dependency: &tg::Dependency) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn get_package_outdated(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::outdated::Arg,
	) -> tg::Result<tg::package::outdated::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn publish_package(
		&self,
		_id: &tg::artifact::Id,
		_arg: tg::package::publish::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_package_versions(
		&self,
		_dependency: &tg::Dependency,
		_arg: tg::package::versions::Arg,
	) -> tg::Result<Option<tg::package::versions::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn yank_package(
		&self,
		_id: &tg::artifact::Id,
		_arg: tg::package::yank::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn list_remotes(
		&self,
		_arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_remote(&self, _name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn put_remote(&self, _name: &str, _arg: tg::remote::put::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn delete_remote(&self, _name: &str) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn list_roots(&self, _arg: tg::root::list::Arg) -> tg::Result<tg::root::list::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_root(&self, _name: &str) -> tg::Result<Option<tg::root::get::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn put_root(&self, _name: &str, _arg: tg::root::put::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn delete_root(&self, _name: &str) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		Err(tg::error!("forbidden"))
	}

	async fn health(&self) -> tg::Result<tg::server::Health> {
		Err(tg::error!("forbidden"))
	}

	async fn clean(&self) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn build_target(
		&self,
		id: &tg::target::Id,
		mut arg: tg::target::build::Arg,
	) -> tg::Result<tg::target::build::Output> {
		arg.parent = Some(self.build.clone());
		arg.remote = self.remote.clone();
		arg.retry = tg::Build::with_id(self.build.clone()).retry(self).await?;
		self.server.build_target(id, arg).await
	}

	async fn get_user(&self, _token: &str) -> tg::Result<Option<tg::User>> {
		Err(tg::error!("forbidden"))
	}
}

impl std::ops::Deref for Proxy {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
