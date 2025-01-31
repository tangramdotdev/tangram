use crate::Server;
use futures::{stream, Future, Stream, TryStreamExt as _};
use std::{ops::Deref, path::PathBuf, pin::Pin, sync::Arc};
use tangram_client as tg;
use tangram_either::Either;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

#[derive(Clone)]
pub struct Proxy(Arc<Inner>);

pub struct Inner {
	process: tg::Process,
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
		process: &tg::Process,
		remote: Option<String>,
		path_map: Option<PathMap>,
	) -> Self {
		Self(Arc::new(Inner {
			process: process.clone(),
			path_map,
			remote,
			server,
		}))
	}

	fn host_path_for_guest_path(&self, path: PathBuf) -> PathBuf {
		// If the path is a tempdir, don't remap.
		#[cfg(target_os = "linux")]
		if path.starts_with("/tmp") {
			return path;
		}

		// Get the path map. If there is no path map, then the guest path is the host path.
		let Some(path_map) = &self.path_map else {
			return path;
		};

		// Map the path.
		if let Ok(path) = path.strip_prefix(&path_map.output_guest) {
			path_map.output_host.join(path)
		} else {
			path_map
				.root_host
				.join(path.strip_prefix("/").unwrap_or(path.as_ref()))
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
		arg.path = self.host_path_for_guest_path(arg.path.clone());

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
			*path = self.host_path_for_guest_path(path.clone());
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
		self.server.create_blob_with_reader(reader)
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

	async fn try_spawn_process(
		&self,
		mut arg: tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		arg.parent = Some(self.process.id().clone());
		arg.remote = self.remote.clone();
		arg.retry = *self.process.retry(self).await?;
		self.server.try_spawn_process(arg).await
	}

	fn wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		self.server.wait_process_future(id)
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

	async fn open_pipe(&self) -> tg::Result<tg::pipe::open::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn close_pipe(&self, _id: &tg::pipe::Id) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		self.server.read_pipe(id)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> {
		self.server.write_pipe(id, stream)
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		self.server.try_get_process(id)
	}

	async fn put_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn push_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn pull_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn try_dequeue_process(
		&self,
		_arg: tg::process::dequeue::Arg,
	) -> tg::Result<Option<tg::process::dequeue::Output>> {
		Err(tg::error!("forbidden"))
	}

	async fn try_start_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::start::Arg,
	) -> tg::Result<tg::process::start::Output> {
		Err(tg::error!("forbidden"))
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		self.server.try_get_process_status_stream(id)
	}

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		arg.remote = self.remote.clone();
		self.server.try_get_process_children_stream(id, arg)
	}

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		arg.remote = self.remote.clone();
		self.server.try_get_process_log_stream(id, arg)
	}

	async fn try_post_process_log(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::log::post::Arg,
	) -> tg::Result<tg::process::log::post::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_finish_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::finish::Arg,
	) -> tg::Result<tg::process::finish::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn touch_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn heartbeat_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn try_get_reference(
		&self,
		_reference: &tg::Reference,
	) -> tg::Result<Option<tg::Referent<Either<tg::process::Id, tg::object::Id>>>> {
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

	async fn get_user(&self, _token: &str) -> tg::Result<Option<tg::User>> {
		Err(tg::error!("forbidden"))
	}
}

impl Deref for Proxy {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
