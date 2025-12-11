use {
	crate::{Context, Owned, Server},
	futures::{Stream, stream::BoxStream},
	tangram_client::prelude::*,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

impl tg::Handle for Owned {
	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.0.cache(arg).await
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.0.check(arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.0.checkin(arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.0.checkout(arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.0.clean().await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		self.0.document(arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.0.format(arg).await
	}

	async fn health(&self) -> tg::Result<tg::Health> {
		self.0.health().await
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.0.index().await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.0.lsp(input, output).await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		self.0.pull(arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		self.0.push(arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		self.0.sync(arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.0.try_get(reference, arg).await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.0.try_read_stream(arg).await
	}

	async fn write(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.0.write(reader).await
	}
}

impl tg::handle::Module for Owned {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.0.resolve_module(arg).await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.0.load_module(arg).await
	}
}

impl tg::handle::Object for Owned {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.0.try_get_object_metadata(id, arg).await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.0.try_get_object(id, arg).await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.0.put_object(id, arg).await
	}

	async fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_object(id, arg).await
	}
}

impl tg::handle::Process for Owned {
	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.0.list_processes(arg).await
	}

	async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		self.0.try_spawn_process(arg).await
	}

	async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		self.0.try_wait_process_future(id, arg).await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.0.try_get_process_metadata(id, arg).await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.0.try_get_process(id, arg).await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		self.0.put_process(id, arg).await
	}

	async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		self.0.cancel_process(id, arg).await
	}

	async fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> tg::Result<Option<tg::process::dequeue::Output>> {
		self.0.try_dequeue_process(arg).await
	}

	async fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> tg::Result<()> {
		self.0.start_process(id, arg).await
	}

	async fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		self.0.heartbeat_process(id, arg).await
	}

	async fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		self.0
			.post_process_signal_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_signal_stream(id, arg).await
	}

	async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_status_stream(id, arg).await
	}

	async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_children_stream(id, arg).await
	}

	async fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_log_stream(id, arg).await
	}

	async fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		self.0.post_process_log(id, arg).await
	}

	async fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		self.0.finish_process(id, arg).await
	}

	async fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_process(id, arg).await
	}
}

impl tg::handle::Pipe for Owned {
	async fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		self.0.create_pipe(arg).await
	}

	async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		self.0.close_pipe(id, arg).await
	}

	async fn delete_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::delete::Arg) -> tg::Result<()> {
		self.0.delete_pipe(id, arg).await
	}

	async fn try_read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>> {
		self.0.try_read_pipe(id, arg).await
	}

	async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> tg::Result<()> {
		self.0.write_pipe(id, arg, stream).await
	}
}

impl tg::handle::Pty for Owned {
	async fn create_pty(&self, arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		self.0.create_pty(arg).await
	}

	async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		self.0.close_pty(id, arg).await
	}

	async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		self.0.delete_pty(id, arg).await
	}

	async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		self.0.get_pty_size(id, arg).await
	}

	async fn try_read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>> {
		self.0.try_read_pty(id, arg).await
	}

	async fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> tg::Result<()> {
		self.0.write_pty(id, arg, stream).await
	}
}

impl tg::handle::Remote for Owned {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.0.list_remotes(arg).await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.0.try_get_remote(name).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.0.put_remote(name, arg).await
	}

	async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		self.0.delete_remote(name).await
	}
}

impl tg::handle::Tag for Owned {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.0.list_tags(arg).await
	}

	async fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		self.0.try_get_tag(pattern, arg).await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.0.put_tag(tag, arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::post::Arg) -> tg::Result<()> {
		self.0.post_tag_batch(arg).await
	}

	async fn delete_tag(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.0.delete_tag(arg).await
	}
}

impl tg::handle::User for Owned {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.0.get_user(token).await
	}
}

impl tg::handle::Watch for Owned {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.0.list_watches(arg).await
	}

	async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		self.0.delete_watch(arg).await
	}
}

impl tg::Handle for Server {
	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.cache_with_context(&Context::default(), arg).await
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.check_with_context(&Context::default(), arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.checkin_with_context(&Context::default(), arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.checkout_with_context(&Context::default(), arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.clean_with_context(&Context::default()).await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		self.document_with_context(&Context::default(), arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.format_with_context(&Context::default(), arg).await
	}

	async fn health(&self) -> tg::Result<tg::Health> {
		self.health_with_context(&Context::default()).await
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.index_with_context(&Context::default()).await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.lsp_with_context(&Context::default(), input, output)
			.await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		self.pull_with_context(&Context::default(), arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		self.push_with_context(&Context::default(), arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		self.sync_with_context(&Context::default(), arg, stream)
			.await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.try_get_with_context(&Context::default(), reference, arg)
			.await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.try_read_stream_with_context(&Context::default(), arg)
			.await
	}

	async fn write(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.write_with_context(&Context::default(), reader).await
	}
}

impl tg::handle::Module for Server {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.resolve_module_with_context(&Context::default(), arg)
			.await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.load_module_with_context(&Context::default(), arg)
			.await
	}
}

impl tg::handle::Object for Server {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.try_get_object_metadata_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.try_get_object_with_context(&Context::default(), id, arg)
			.await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.put_object_with_context(&Context::default(), id, arg)
			.await
	}

	async fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		self.touch_object_with_context(&Context::default(), id, arg)
			.await
	}
}

impl tg::handle::Process for Server {
	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.list_processes_with_context(&Context::default(), arg)
			.await
	}

	async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		self.try_spawn_process_with_context(&Context::default(), arg)
			.await
	}

	async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		self.try_wait_process_future_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.try_get_process_metadata_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.try_get_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		self.put_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		self.cancel_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> tg::Result<Option<tg::process::dequeue::Output>> {
		self.try_dequeue_process_with_context(&Context::default(), arg)
			.await
	}

	async fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> tg::Result<()> {
		self.start_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		self.heartbeat_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		self.post_process_signal_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static>,
	> {
		self.try_get_process_signal_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.try_get_process_status_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.try_get_process_children_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		self.try_get_process_log_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		self.post_process_log_with_context(&Context::default(), id, arg)
			.await
	}

	async fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		self.finish_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		self.touch_process_with_context(&Context::default(), id, arg)
			.await
	}
}

impl tg::handle::Pipe for Server {
	async fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		self.create_pipe_with_context(&Context::default(), arg)
			.await
	}

	async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		self.close_pipe_with_context(&Context::default(), id, arg)
			.await
	}

	async fn delete_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::delete::Arg) -> tg::Result<()> {
		self.delete_pipe_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>> {
		self.try_read_pipe_with_context(&Context::default(), id, arg)
			.await
	}

	async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> tg::Result<()> {
		self.write_pipe_with_context(&Context::default(), id, arg, stream)
			.await
	}
}

impl tg::handle::Pty for Server {
	async fn create_pty(&self, arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		self.create_pty_with_context(&Context::default(), arg).await
	}

	async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		self.close_pty_with_context(&Context::default(), id, arg)
			.await
	}

	async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		self.delete_pty_with_context(&Context::default(), id, arg)
			.await
	}

	async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		self.try_get_pty_size_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>> {
		self.try_read_pty_with_context(&Context::default(), id, arg)
			.await
	}

	async fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> tg::Result<()> {
		self.write_pty_with_context(&Context::default(), id, arg, stream)
			.await
	}
}

impl tg::handle::Remote for Server {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.list_remotes_with_context(&Context::default(), arg)
			.await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.try_get_remote_with_context(&Context::default(), name)
			.await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.put_remote_with_context(&Context::default(), name, arg)
			.await
	}

	async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		self.delete_remote_with_context(&Context::default(), name)
			.await
	}
}

impl tg::handle::Tag for Server {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.list_tags_with_context(&Context::default(), arg).await
	}

	async fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		self.try_get_tag_with_context(&Context::default(), pattern, arg)
			.await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.put_tag_with_context(&Context::default(), tag, arg)
			.await
	}

	async fn post_tag_batch(&self, arg: tg::tag::post::Arg) -> tg::Result<()> {
		self.post_tag_batch_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_tag(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.delete_tag_with_context(&Context::default(), arg).await
	}
}

impl tg::handle::User for Server {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.get_user_with_context(&Context::default(), token).await
	}
}

impl tg::handle::Watch for Server {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.list_watches_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		self.delete_watch_with_context(&Context::default(), arg)
			.await
	}
}

#[derive(Clone)]
#[cfg_attr(not(feature = "js"), expect(dead_code))]
pub struct ServerWithContext(pub Server, pub Context);

impl tg::Handle for ServerWithContext {
	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.0.cache_with_context(&self.1, arg).await
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.0.check_with_context(&self.1, arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.0.checkin_with_context(&self.1, arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.0.checkout_with_context(&self.1, arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.0.clean_with_context(&self.1).await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		self.0.document_with_context(&self.1, arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.0.format_with_context(&self.1, arg).await
	}

	async fn health(&self) -> tg::Result<tg::Health> {
		self.0.health_with_context(&self.1).await
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.0.index_with_context(&self.1).await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.0.lsp_with_context(&self.1, input, output).await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		self.0.pull_with_context(&self.1, arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		self.0.push_with_context(&self.1, arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		self.0.sync_with_context(&self.1, arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.0.try_get_with_context(&self.1, reference, arg).await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.0.try_read_stream_with_context(&self.1, arg).await
	}

	async fn write(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.0.write_with_context(&self.1, reader).await
	}
}

impl tg::handle::Module for ServerWithContext {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.0.resolve_module_with_context(&self.1, arg).await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.0.load_module_with_context(&self.1, arg).await
	}
}

impl tg::handle::Object for ServerWithContext {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.0
			.try_get_object_metadata_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.0.try_get_object_with_context(&self.1, id, arg).await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.0.put_object_with_context(&self.1, id, arg).await
	}

	async fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_object_with_context(&self.1, id, arg).await
	}
}

impl tg::handle::Process for ServerWithContext {
	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.0.list_processes_with_context(&self.1, arg).await
	}

	async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<Option<tg::process::spawn::Output>> {
		self.0.try_spawn_process_with_context(&self.1, arg).await
	}

	async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		self.0
			.try_wait_process_future_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.0
			.try_get_process_metadata_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.0.try_get_process_with_context(&self.1, id, arg).await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		self.0.put_process_with_context(&self.1, id, arg).await
	}

	async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		self.0.cancel_process_with_context(&self.1, id, arg).await
	}

	async fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> tg::Result<Option<tg::process::dequeue::Output>> {
		self.0.try_dequeue_process_with_context(&self.1, arg).await
	}

	async fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> tg::Result<()> {
		self.0.start_process_with_context(&self.1, id, arg).await
	}

	async fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> tg::Result<tg::process::heartbeat::Output> {
		self.0
			.heartbeat_process_with_context(&self.1, id, arg)
			.await
	}

	async fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		self.0
			.post_process_signal_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static>,
	> {
		self.0
			.try_get_process_signal_stream_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.0
			.try_get_process_status_stream_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.0
			.try_get_process_children_stream_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
	> {
		self.0
			.try_get_process_log_stream_with_context(&self.1, id, arg)
			.await
	}

	async fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		self.0.post_process_log_with_context(&self.1, id, arg).await
	}

	async fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		self.0.finish_process_with_context(&self.1, id, arg).await
	}

	async fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_process_with_context(&self.1, id, arg).await
	}
}

impl tg::handle::Pipe for ServerWithContext {
	async fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		self.0.create_pipe_with_context(&self.1, arg).await
	}

	async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		self.0.close_pipe_with_context(&self.1, id, arg).await
	}

	async fn delete_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::delete::Arg) -> tg::Result<()> {
		self.0.delete_pipe_with_context(&self.1, id, arg).await
	}

	async fn try_read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>> {
		self.0.try_read_pipe_with_context(&self.1, id, arg).await
	}

	async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> tg::Result<()> {
		self.0
			.write_pipe_with_context(&self.1, id, arg, stream)
			.await
	}
}

impl tg::handle::Pty for ServerWithContext {
	async fn create_pty(&self, arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		self.0.create_pty_with_context(&self.1, arg).await
	}

	async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		self.0.close_pty_with_context(&self.1, id, arg).await
	}

	async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		self.0.delete_pty_with_context(&self.1, id, arg).await
	}

	async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		self.0.try_get_pty_size_with_context(&self.1, id, arg).await
	}

	async fn try_read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>> {
		self.0.try_read_pty_with_context(&self.1, id, arg).await
	}

	async fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> tg::Result<()> {
		self.0
			.write_pty_with_context(&self.1, id, arg, stream)
			.await
	}
}

impl tg::handle::Remote for ServerWithContext {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.0.list_remotes_with_context(&self.1, arg).await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.0.try_get_remote_with_context(&self.1, name).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.0.put_remote_with_context(&self.1, name, arg).await
	}

	async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		self.0.delete_remote_with_context(&self.1, name).await
	}
}

impl tg::handle::Tag for ServerWithContext {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.0.list_tags_with_context(&self.1, arg).await
	}

	async fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		self.0.try_get_tag_with_context(&self.1, pattern, arg).await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.0.put_tag_with_context(&self.1, tag, arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::post::Arg) -> tg::Result<()> {
		self.0.post_tag_batch_with_context(&self.1, arg).await
	}

	async fn delete_tag(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.0.delete_tag_with_context(&self.1, arg).await
	}
}

impl tg::handle::User for ServerWithContext {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.0.get_user_with_context(&self.1, token).await
	}
}

impl tg::handle::Watch for ServerWithContext {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.0.list_watches_with_context(&self.1, arg).await
	}

	async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		self.0.delete_watch_with_context(&self.1, arg).await
	}
}
