use crate as tg;
use futures::{FutureExt as _, Stream, TryFutureExt as _};
use std::pin::Pin;
use tangram_either::Either;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

impl<L, R> tg::Handle for Either<L, R>
where
	L: tg::Handle,
	R: tg::Handle,
{
	fn check(
		&self,
		arg: tg::check::Arg,
	) -> impl Future<Output = tg::Result<tg::check::Output>> + Send {
		match self {
			Either::Left(s) => s.check(arg).left_future(),
			Either::Right(s) => s.check(arg).right_future(),
		}
	}

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.checkin(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.checkin(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.checkout(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.checkout(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.clean()
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.clean()
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn document(
		&self,
		arg: tg::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send {
		match self {
			Either::Left(s) => s.document(arg).left_future(),
			Either::Right(s) => s.document(arg).right_future(),
		}
	}

	fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>,
	> {
		match self {
			Either::Left(s) => s
				.export(arg, stream)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.export(arg, stream)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn format(&self, arg: tg::format::Arg) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.format(arg).left_future(),
			Either::Right(s) => s.format(arg).right_future(),
		}
	}

	fn health(&self) -> impl Future<Output = tg::Result<tg::Health>> {
		match self {
			Either::Left(s) => s.health().left_future(),
			Either::Right(s) => s.health().right_future(),
		}
	}

	fn import(
		&self,
		arg: tg::import::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>,
	> {
		match self {
			Either::Left(s) => s
				.import(arg, stream)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.import(arg, stream)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.index()
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.index()
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.lsp(input, output).left_future(),
			Either::Right(s) => s.lsp(input, output).right_future(),
		}
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.pull(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.pull(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		match self {
			Either::Left(s) => s
				.push(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.push(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> {
		match self {
			Either::Left(s) => s.create_blob(reader).left_future(),
			Either::Right(s) => s.create_blob(reader).right_future(),
		}
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
		match self {
			Either::Left(s) => s
				.try_read_blob_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_read_blob_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get(
		&self,
		reference: &tg::Reference,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.try_get(reference)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.try_get(reference)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}
}

impl<L, R> tg::handle::Object for Either<L, R>
where
	L: tg::handle::Object,
	R: tg::handle::Object,
{
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		match self {
			Either::Left(s) => s.try_get_object_metadata(id).left_future(),
			Either::Right(s) => s.try_get_object_metadata(id).right_future(),
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_object(id).left_future(),
			Either::Right(s) => s.try_get_object(id).right_future(),
		}
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_object(id, arg).left_future(),
			Either::Right(s) => s.put_object(id, arg).right_future(),
		}
	}

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.touch_object(id, arg).left_future(),
			Either::Right(s) => s.touch_object(id, arg).right_future(),
		}
	}
}

impl<L, R> tg::handle::Process for Either<L, R>
where
	L: tg::handle::Process,
	R: tg::handle::Process,
{
	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		match self {
			Either::Left(s) => s.try_get_process_metadata(id).left_future(),
			Either::Right(s) => s.try_get_process_metadata(id).right_future(),
		}
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_process(id).left_future(),
			Either::Right(s) => s.try_get_process(id).right_future(),
		}
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_process(id, arg).left_future(),
			Either::Right(s) => s.put_process(id, arg).right_future(),
		}
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
		match self {
			Either::Left(s) => s
				.try_get_process_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_process_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
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
		match self {
			Either::Left(s) => s
				.try_get_process_log_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_process_log_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
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
		match self {
			Either::Left(s) => s
				.try_get_process_signal_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_process_signal_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		match self {
			Either::Left(s) => s
				.try_get_process_status_stream(id)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			Either::Right(s) => s
				.try_get_process_status_stream(id)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn cancel_process(
		&self,
		id: &crate::process::Id,
		arg: crate::process::cancel::Arg,
	) -> impl Future<Output = crate::Result<()>> + Send {
		match self {
			Either::Left(s) => s.cancel_process(id, arg).left_future(),
			Either::Right(s) => s.cancel_process(id, arg).right_future(),
		}
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::dequeue::Output>>> {
		match self {
			Either::Left(s) => s.try_dequeue_process(arg).left_future(),
			Either::Right(s) => s.try_dequeue_process(arg).right_future(),
		}
	}

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.finish_process(id, arg).left_future(),
			Either::Right(s) => s.finish_process(id, arg).right_future(),
		}
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> + Send {
		match self {
			Either::Left(s) => s.heartbeat_process(id, arg).left_future(),
			Either::Right(s) => s.heartbeat_process(id, arg).right_future(),
		}
	}

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.post_process_log(id, arg).left_future(),
			Either::Right(s) => s.post_process_log(id, arg).right_future(),
		}
	}

	fn signal_process(
		&self,
		id: &crate::process::Id,
		arg: crate::process::signal::post::Arg,
	) -> impl Future<Output = crate::Result<()>> + Send {
		match self {
			Either::Left(s) => s.signal_process(id, arg).left_future(),
			Either::Right(s) => s.signal_process(id, arg).right_future(),
		}
	}

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::spawn::Output>>> {
		match self {
			Either::Left(s) => s.try_spawn_process(arg).left_future(),
			Either::Right(s) => s.try_spawn_process(arg).right_future(),
		}
	}

	fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.start_process(id, arg).left_future(),
			Either::Right(s) => s.start_process(id, arg).right_future(),
		}
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Either::Left(s) => s.touch_process(id, arg).left_future(),
			Either::Right(s) => s.touch_process(id, arg).right_future(),
		}
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
		match self {
			Either::Left(s) => s
				.try_wait_process_future(id)
				.map_ok(|option| option.map(futures::FutureExt::left_future))
				.left_future(),
			Either::Right(s) => s
				.try_wait_process_future(id)
				.map_ok(|option| option.map(futures::FutureExt::right_future))
				.right_future(),
		}
	}
}

impl<L, R> tg::handle::Pipe for Either<L, R>
where
	L: tg::handle::Pipe,
	R: tg::handle::Pipe,
{
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		match self {
			Either::Left(s) => s.create_pipe(arg).left_future(),
			Either::Right(s) => s.create_pipe(arg).right_future(),
		}
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.close_pipe(id, arg).left_future(),
			Either::Right(s) => s.close_pipe(id, arg).right_future(),
		}
	}

	fn read_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>
	{
		match self {
			Either::Left(s) => s
				.read_pipe(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.read_pipe(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.write_pipe(id, arg, stream).left_future(),
			Either::Right(s) => s.write_pipe(id, arg, stream).right_future(),
		}
	}
}

impl<L, R> tg::handle::Pty for Either<L, R>
where
	L: tg::handle::Pty,
	R: tg::handle::Pty,
{
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		match self {
			Either::Left(s) => s.create_pty(arg).left_future(),
			Either::Right(s) => s.create_pty(arg).right_future(),
		}
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.close_pty(id, arg).left_future(),
			Either::Right(s) => s.close_pty(id, arg).right_future(),
		}
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		match self {
			Either::Left(s) => s.get_pty_size(id, arg).left_future(),
			Either::Right(s) => s.get_pty_size(id, arg).right_future(),
		}
	}

	fn read_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<Output = tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>
	{
		match self {
			Either::Left(s) => s
				.read_pty(id, arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			Either::Right(s) => s
				.read_pty(id, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.write_pty(id, arg, stream).left_future(),
			Either::Right(s) => s.write_pty(id, arg, stream).right_future(),
		}
	}
}

impl<L, R> tg::handle::Remote for Either<L, R>
where
	L: tg::handle::Remote,
	R: tg::handle::Remote,
{
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		match self {
			Either::Left(s) => s.list_remotes(arg).left_future(),
			Either::Right(s) => s.list_remotes(arg).right_future(),
		}
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_remote(name).left_future(),
			Either::Right(s) => s.try_get_remote(name).right_future(),
		}
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_remote(name, arg).left_future(),
			Either::Right(s) => s.put_remote(name, arg).right_future(),
		}
	}

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.delete_remote(name).left_future(),
			Either::Right(s) => s.delete_remote(name).right_future(),
		}
	}
}

impl<L, R> tg::handle::Tag for Either<L, R>
where
	L: tg::handle::Tag,
	R: tg::handle::Tag,
{
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		match self {
			Either::Left(s) => s.list_tags(arg).left_future(),
			Either::Right(s) => s.list_tags(arg).right_future(),
		}
	}

	fn try_get_tag(
		&self,
		pattern: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		match self {
			Either::Left(s) => s.try_get_tag(pattern).left_future(),
			Either::Right(s) => s.try_get_tag(pattern).right_future(),
		}
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.put_tag(tag, arg).left_future(),
			Either::Right(s) => s.put_tag(tag, arg).right_future(),
		}
	}

	fn delete_tag(&self, tag: &tg::Tag) -> impl Future<Output = tg::Result<()>> {
		match self {
			Either::Left(s) => s.delete_tag(tag).left_future(),
			Either::Right(s) => s.delete_tag(tag).right_future(),
		}
	}
}

impl<L, R> tg::handle::User for Either<L, R>
where
	L: tg::handle::User,
	R: tg::handle::User,
{
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			Either::Left(s) => s.get_user(token).left_future(),
			Either::Right(s) => s.get_user(token).right_future(),
		}
	}
}
