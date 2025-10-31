use {
	crate::{Context, Handle, Server, run::proxy::Proxy},
	futures::{FutureExt, Stream, stream::BoxStream},
	std::ops::Deref,
	tangram_client::{self as tg, handle::Tag as _},
	tangram_either::Either,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

#[derive(Clone)]
pub struct ServerOrProxy(pub Either<Server, Proxy>);

impl Deref for ServerOrProxy {
	type Target = Either<Server, Proxy>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tg::Handle for Handle {
	fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.0.cache(arg)
	}

	fn check(&self, arg: tg::check::Arg) -> impl Future<Output = tg::Result<tg::check::Output>> {
		self.0.check(arg)
	}

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
		>,
	> {
		self.0.checkin(arg)
	}

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
		>,
	> {
		self.0.checkout(arg)
	}

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
		>,
	> + Send {
		self.0.clean()
	}

	fn document(
		&self,
		arg: tg::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.0.document(arg)
	}

	fn format(&self, arg: tg::format::Arg) -> impl Future<Output = tg::Result<()>> {
		self.0.format(arg)
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		self.0.health()
	}

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		self.0.index()
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.lsp(input, output)
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
		>,
	> {
		self.0.pull(arg)
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
		>,
	> {
		self.0.push(arg)
	}

	fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static>,
	> {
		self.0.sync(arg, stream)
	}

	fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send {
		self.0.try_get(reference, arg)
	}

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>,
		>,
	> {
		self.0.try_read_stream(arg)
	}

	fn write(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::write::Output>> {
		self.0.write(reader)
	}
}

impl tg::handle::Object for Handle {
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.0.try_get_object_metadata(id, arg)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.0.try_get_object(id, arg)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.put_object(id, arg)
	}

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.touch_object(id, arg)
	}
}

impl tg::handle::Process for Handle {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> + Send {
		self.0.list_processes(arg)
	}

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::spawn::Output>>> {
		self.0.try_spawn_process(arg)
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
		self.0.try_wait_process_future(id)
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		self.0.try_get_process_metadata(id, arg)
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		self.0.try_get_process(id, arg)
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.put_process(id, arg)
	}

	fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.0.cancel_process(id, arg)
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> {
		self.0.try_dequeue_process(arg)
	}

	fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.start_process(id, arg)
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> {
		self.0.heartbeat_process(id, arg)
	}

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.0.post_process_signal(id, arg)
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
		self.0.try_get_process_signal_stream(id, arg)
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		self.0.try_get_process_status_stream(id)
	}

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.0.try_get_process_children_stream(id, arg)
	}

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.0.try_get_process_log_stream(id, arg)
	}

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.post_process_log(id, arg)
	}

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.finish_process(id, arg)
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.touch_process(id, arg)
	}
}

impl tg::handle::Pipe for Handle {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		self.0.create_pipe(arg)
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.close_pipe(id, arg)
	}

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.delete_pipe(id, arg)
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		self.0.read_pipe(id, arg)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.0.write_pipe(id, arg, stream)
	}
}

impl tg::handle::Pty for Handle {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		self.0.create_pty(arg)
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.close_pty(id, arg)
	}

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.delete_pty(id, arg)
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		self.0.get_pty_size(id, arg)
	}

	fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>
	{
		self.0.read_pty(id, arg)
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.0.write_pty(id, arg, stream)
	}
}

impl tg::handle::Remote for Handle {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		self.0.list_remotes(arg)
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		self.0.try_get_remote(name)
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.put_remote(name, arg)
	}

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		self.0.delete_remote(name)
	}
}

impl tg::handle::Tag for Handle {
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		self.0.list_tags(arg)
	}

	fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		self.0.try_get_tag(pattern)
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		let context = Context { token: None };
		self.0.put_tag(context, tag, arg)
	}

	fn post_tags_batch(
		&self,
		arg: tg::tag::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let context = Context { token: None };
		self.0.post_tags_batch(context, arg)
	}

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.0.delete_tag(arg)
	}
}

impl tg::handle::User for Handle {
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.0.get_user(token)
	}
}

impl tg::handle::Watch for Handle {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> {
		self.0.list_watches(arg)
	}

	fn delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.delete_watch(arg)
	}
}

impl tg::Handle for Server {
	fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.cache(arg)
	}

	fn check(&self, arg: tg::check::Arg) -> impl Future<Output = tg::Result<tg::check::Output>> {
		self.check(arg)
	}

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
		>,
	> {
		self.checkin(arg)
	}

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
		>,
	> {
		self.checkout(arg)
	}

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
		>,
	> + Send {
		self.clean()
	}

	fn document(
		&self,
		arg: tg::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> {
		self.document(arg)
	}

	fn format(&self, arg: tg::format::Arg) -> impl Future<Output = tg::Result<()>> {
		self.format(arg)
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		self.health()
	}

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		self.index()
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.lsp(input, output)
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
		>,
	> {
		self.pull(arg)
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
		>,
	> {
		self.push(arg)
	}

	fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static>,
	> {
		self.sync(arg, stream)
	}

	fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send {
		self.try_get(reference, arg)
	}

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>,
		>,
	> {
		self.try_read_stream(arg)
	}

	fn write(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::write::Output>> {
		self.write(reader)
	}
}

impl tg::handle::Object for Server {
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id, arg)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id, arg)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_object(id, arg)
	}

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_object(id, arg)
	}
}

impl tg::handle::Process for Server {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> + Send {
		self.list_processes(arg)
	}

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::spawn::Output>>> {
		self.try_spawn_process(arg)
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
		self.try_wait_process_future(id)
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata(id, arg)
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process(id, arg)
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_process(id, arg)
	}

	fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.cancel_process(id, arg)
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> {
		self.try_dequeue_process(arg)
	}

	fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.start_process(id, arg)
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> {
		self.heartbeat_process(id, arg)
	}

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.post_process_signal(id, arg)
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
		self.try_get_process_signal_stream(id, arg)
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_status_stream(id)
	}

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.try_get_process_children_stream(id, arg)
	}

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_log_stream(id, arg)
	}

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.post_process_log(id, arg)
	}

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.finish_process(id, arg)
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_process(id, arg)
	}
}

impl tg::handle::Pipe for Server {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		self.create_pipe(arg)
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_pipe(id, arg)
	}

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.delete_pipe(id, arg)
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		self.read_pipe(id, arg)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.write_pipe(id, arg, stream)
	}
}

impl tg::handle::Pty for Server {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		self.create_pty(arg)
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_pty(id, arg)
	}

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.delete_pty(id, arg)
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		self.get_pty_size(id, arg)
	}

	fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>
	{
		self.read_pty(id, arg)
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.write_pty(id, arg, stream)
	}
}

impl tg::handle::Remote for Server {
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
}

impl tg::handle::Tag for Server {
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
		let context = Context { token: None };
		self.put_tag(context, tag, arg)
	}

	fn post_tags_batch(&self, arg: tg::tag::post::Arg) -> impl Future<Output = tg::Result<()>> {
		let context = Context { token: None };
		self.post_tags_batch(context, arg)
	}

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.delete_tag(arg)
	}
}

impl tg::handle::User for Server {
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_user(token)
	}
}

impl tg::handle::Watch for Server {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> {
		self.list_watches(arg)
	}

	fn delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.delete_watch(arg)
	}
}

impl ServerOrProxy {
	pub fn put_tag(
		&self,
		cx: Context,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match &self.0 {
			Either::Left(server) => server.put_tag(cx, tag, arg).left_future(),
			Either::Right(proxy) => proxy.put_tag(tag, arg).right_future(),
		}
	}

	pub fn post_tags_batch(
		&self,
		context: Context,
		arg: tg::tag::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match &self.0 {
			Either::Left(server) => server.post_tags_batch(context, arg).left_future(),
			Either::Right(proxy) => proxy.post_tags_batch(arg).right_future(),
		}
	}
}
