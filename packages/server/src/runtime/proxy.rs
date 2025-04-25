use crate::Server;
use futures::{Stream, TryStreamExt as _, stream};
use std::{ops::Deref, path::PathBuf, pin::Pin, sync::Arc};
use tangram_client as tg;
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
	async fn health(&self) -> tg::Result<tg::Health> {
		Err(tg::error!("forbidden"))
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn clean(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn check(&self, _arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn document(&self, _arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		Err(tg::error!("forbidden"))
	}

	async fn format(&self, _arg: tg::format::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn checkin(
		&self,
		mut arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		// Replace the path with the host path.
		arg.path = self.host_path_for_guest_path(arg.path.clone());

		// Perform the checkin.
		let stream = self.server.checkin(arg).await?;

		Ok(stream)
	}

	async fn checkout(
		&self,
		mut arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		// Replace the path with the host path.
		if let Some(path) = &mut arg.path {
			*path = self.host_path_for_guest_path(path.clone());
		}

		// Check out the artifact.
		let stream = self.server.checkout(arg).await?;

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

	async fn import(
		&self,
		_arg: tg::import::Arg,
		_stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}

	async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		self.server.export(arg, stream).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.server.push(arg).await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.server.pull(arg).await
	}

	async fn lsp(
		&self,
		_input: impl AsyncBufRead + Send + Unpin + 'static,
		_output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
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

	async fn try_get(
		&self,
		_reference: &tg::Reference,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		Err::<stream::Empty<_>, _>(tg::error!("forbidden"))
	}
}

impl tg::handle::Object for Proxy {
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

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.server.touch_object(id, arg)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.server.put_object(id, arg)
	}
}

impl tg::handle::Process for Proxy {
	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		self.server.try_get_process_metadata(id)
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
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
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

	fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		self.server.try_wait_process_future(id)
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

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.server.post_process_signal(id, arg)
	}

	fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.server.try_get_process_signal_stream(id, arg)
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
	) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	async fn try_finish_process(
		&self,
		_id: &tg::process::Id,
		_arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
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
}

impl tg::handle::Pipe for Proxy {
	async fn create_pipe(
		&self,
		_arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn close_pipe(&self, _id: &tg::pipe::Id, _arg: tg::pipe::close::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		self.server.read_pipe(id, arg)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> {
		self.server.write_pipe(id, arg, stream)
	}
}

impl tg::handle::Pty for Proxy {
	async fn create_pty(&self, _arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		Err(tg::error!("forbidden"))
	}

	async fn close_pty(&self, _id: &tg::pty::Id, _arg: tg::pty::close::Arg) -> tg::Result<()> {
		Err(tg::error!("forbidden"))
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		self.server.get_pty_size(id, arg)
	}

	fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>
	{
		self.server.read_pty(id, arg)
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> {
		self.server.write_pty(id, arg, stream)
	}
}

impl tg::handle::Remote for Proxy {
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
}

impl tg::handle::Tag for Proxy {
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
}

impl tg::handle::User for Proxy {
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
