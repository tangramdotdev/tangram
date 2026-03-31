use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
};

pub trait Process: Send + Sync + 'static {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::process::list::Output>>;

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<
			BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
		>,
	>;

	fn try_get_process_metadata<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Metadata>>>;

	fn try_get_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>>;

	fn put_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn cancel_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_dequeue_process(
		&self,
		arg: tg::process::queue::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::process::queue::Output>>>;

	fn signal_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_process_signal_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>>,
	>;

	fn try_get_process_status_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>>>;

	fn try_get_process_children_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>>,
	>;

	fn try_get_process_tty_size_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::tty::size::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>>,
	>;

	fn set_process_tty_size<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_read_process_stdio<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>>,
	>;

	fn write_process_stdio<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>>;

	fn heartbeat_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> BoxFuture<'a, tg::Result<tg::process::heartbeat::Output>>;

	fn touch_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn finish_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_wait_process_future<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>>,
	>;
}

impl<T> Process for T
where
	T: tg::handle::Process,
{
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::process::list::Output>> {
		self.list_processes(arg).boxed()
	}

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<
			BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
		>,
	> {
		self.try_spawn_process(arg)
			.map_ok(futures::StreamExt::boxed)
			.boxed()
	}

	fn try_get_process_metadata<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata(id, arg).boxed()
	}

	fn try_get_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process(id, arg).boxed()
	}

	fn put_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_process(id, arg).boxed()
	}

	fn cancel_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.cancel_process(id, arg).boxed()
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::queue::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::process::queue::Output>>> {
		self.try_dequeue_process(arg).boxed()
	}

	fn signal_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.signal_process(id, arg).boxed()
	}

	fn try_get_process_signal_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>>,
	> {
		self.try_get_process_signal_stream(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn try_get_process_status_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>>>
	{
		self.try_get_process_status_stream(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn try_get_process_children_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>>,
	> {
		self.try_get_process_children_stream(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn try_get_process_tty_size_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::tty::size::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::tty::size::get::Event>>>>,
	> {
		self.try_get_process_tty_size_stream(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn set_process_tty_size<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.set_process_tty_size(id, arg).boxed()
	}

	fn try_read_process_stdio<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>>,
	> {
		self.try_read_process_stdio(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn write_process_stdio<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>>
	{
		self.write_process_stdio(id, arg, stream)
			.map_ok(futures::StreamExt::boxed)
			.boxed()
	}

	fn heartbeat_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> BoxFuture<'a, tg::Result<tg::process::heartbeat::Output>> {
		self.heartbeat_process(id, arg).boxed()
	}

	fn touch_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.touch_process(id, arg).boxed()
	}

	fn finish_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.finish_process(id, arg).boxed()
	}

	fn try_wait_process_future<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>>,
	> {
		self.try_wait_process_future(id, arg)
			.map_ok(|option| option.map(futures::FutureExt::boxed))
			.boxed()
	}
}
