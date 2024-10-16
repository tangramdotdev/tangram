use crate::Server;
use futures::{stream, Future, Stream, TryStreamExt as _};
use std::{path::PathBuf, sync::Arc};
use tangram_client as tg;
use tg::path::Ext as _;
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
	pub output_guest: PathBuf,
	pub output_host: PathBuf,
	pub root_host: PathBuf,
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

	fn host_path_for_guest_path(&self, path: PathBuf) -> tg::Result<PathBuf> {
		// Get the path map. If there is no path map, then the guest path is the host path.
		let Some(path_map) = &self.path_map else {
			return Ok(path);
		};

		// Map the path.
		if let Some(path) = path
			.diff(&path_map.output_guest)
			.filter(|path| matches!(path.components().next(), Some(std::path::Component::CurDir)))
		{
			Ok(path_map.output_host.join(path))
		} else {
			let path = path
				.diff("/")
				.ok_or_else(|| tg::error!("the path must be absolute"))?;
			Ok(path_map.root_host.join(path))
		}
	}

	fn guest_path_for_host_path(&self, path: PathBuf) -> tg::Result<PathBuf> {
		// Get the path map. If there is no path map, then the host path is the guest path.
		let Some(path_map) = &self.path_map else {
			return Ok(path);
		};

		// Map the path.
		let result = if path.starts_with(&path_map.output_host) {
			let suffix = path.strip_prefix(&path_map.output_host).unwrap();
			path_map.output_guest.join(suffix)
		} else if path.starts_with(&self.server.path) {
			let suffix = path.strip_prefix(&self.server.path).unwrap();
			PathBuf::from("/.tangram").join(suffix)
		} else {
			let suffix = path.strip_prefix(&path_map.root_host).map_err(|error| {
				tg::error!(source = error, "cannot map path outside of host root")
			})?;
			PathBuf::from("/").join(suffix)
		};

		Ok(result)
	}
}

impl tg::Handle for Proxy {
	async fn check_in_artifact(
		&self,
		mut arg: tg::artifact::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
			+ Send
			+ 'static,
	> {
		// Replace the path with the host path.
		arg.path = self.host_path_for_guest_path(arg.path.clone())?;

		// Perform the checkin.
		let stream = self.server.check_in_artifact(arg).await?;

		Ok(stream)
	}

	async fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		mut arg: tg::artifact::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkout::Output>>>
			+ Send
			+ 'static,
	> {
		// Replace the path with the host path.
		if let Some(path) = &mut arg.path {
			*path = self.host_path_for_guest_path(path.clone())?;
		}

		// Check out the artifact.
		let stream = self.server.check_out_artifact(id, arg).await?;

		// Replace the path with the guest path.
		let proxy = self.clone();
		let stream = stream.and_then(move |mut event| {
			let proxy = proxy.clone();
			async move {
				if let tg::progress::Event::Output(output) = &mut event {
					output.path = proxy.guest_path_for_host_path(output.path.clone())?;
					return Ok(event);
				}
				Ok(event)
			}
		});

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
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn pull_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn try_dequeue_build(
		&self,
		_arg: tg::build::dequeue::Arg,
	) -> tg::Result<Option<tg::build::dequeue::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn try_start_build(
		&self,
		_id: &tg::build::Id,
		_arg: tg::build::start::Arg,
	) -> tg::Result<bool> {
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
	) -> tg::Result<bool> {
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
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn pull_object(
		&self,
		_id: &tg::object::Id,
		_arg: tg::object::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn check_package(
		&self,
		_arg: tg::package::check::Arg,
	) -> tg::Result<tg::package::check::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn document_package(
		&self,
		_arg: tg::package::document::Arg,
	) -> tg::Result<serde_json::Value> {
		Err(tg::error!("forbidden"))
	}

	async fn format_package(&self, _arg: tg::package::format::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_reference(
		&self,
		_reference: &tg::Reference,
	) -> tg::Result<Option<tg::reference::get::Output>> {
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

	async fn get_js_runtime_doc(&self) -> tg::Result<serde_json::Value> {
		Err(tg::error!("forbidden"))
	}

	async fn health(&self) -> tg::Result<tg::Health> {
		Err(tg::error!("forbidden"))
	}

	async fn clean(&self) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn list_tags(&self, _arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_tag(
		&self,
		_pattern: &tg::tag::Pattern,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn put_tag(&self, _tag: &tg::Tag, _arg: tg::tag::put::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn delete_tag(&self, _tag: &tg::Tag) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_build_target(
		&self,
		id: &tg::target::Id,
		mut arg: tg::target::build::Arg,
	) -> tg::Result<Option<tg::target::build::Output>> {
		arg.parent = Some(self.build.clone());
		arg.remote = self.remote.clone();
		arg.retry = tg::Build::with_id(self.build.clone()).retry(self).await?;
		self.server.try_build_target(id, arg).await
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
