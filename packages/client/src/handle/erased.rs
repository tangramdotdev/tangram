use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
	tangram_futures::{BoxAsyncBufRead, BoxAsyncRead, BoxAsyncWrite},
};

pub trait Handle:
	Module + Object + Process + Pipe + Pty + Remote + Tag + User + Watch + Send + Sync + 'static
{
	fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> BoxFuture<'_, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>>;

	fn check(&self, arg: tg::check::Arg) -> BoxFuture<'_, tg::Result<tg::check::Output>>;

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkin::Output>>>>,
	>;

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>>,
	>;

	fn clean(
		&self,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::clean::Output>>>>,
	>;

	fn document(&self, arg: tg::document::Arg) -> BoxFuture<'_, tg::Result<serde_json::Value>>;

	fn format(&self, arg: tg::format::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn health(&self) -> BoxFuture<'_, tg::Result<tg::Health>>;

	fn index(
		&self,
	) -> BoxFuture<'_, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>>;

	fn lsp<'a>(
		&'a self,
		input: BoxAsyncBufRead<'static>,
		output: BoxAsyncWrite<'static>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::pull::Output>>>>,
	>;

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>>,
	>;

	fn sync<'a>(
		&'a self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::sync::Message>>>>;

	fn try_get<'a>(
		&'a self,
		reference: &'a tg::Reference,
		arg: tg::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>>,
	>;

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> BoxFuture<'_, tg::Result<Option<BoxStream<'static, tg::Result<tg::read::Event>>>>>;

	fn write<'a>(
		&'a self,
		reader: BoxAsyncRead<'static>,
	) -> BoxFuture<'a, tg::Result<tg::write::Output>>;
}

pub trait Module: Send + Sync + 'static {
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::resolve::Output>>;

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::load::Output>>;
}

pub trait Object: Send + Sync + 'static {
	fn try_get_object_metadata<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Metadata>>>;

	fn try_get_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>>;

	fn put_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_object_batch(&self, arg: tg::object::batch::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn touch_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;
}

pub trait Process: Send + Sync + 'static {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::process::list::Output>>;

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

	fn try_get_process_children_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>>,
	>;

	fn try_get_process_log_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::log::get::Event>>>>,
	>;

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

	fn cancel_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::process::dequeue::Output>>>;

	fn finish_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn heartbeat_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> BoxFuture<'a, tg::Result<tg::process::heartbeat::Output>>;

	fn post_process_log<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn signal_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<
			BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>,
		>,
	>;

	fn start_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::start::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn touch_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::touch::Arg,
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

pub trait Pipe: Send + Sync + 'static {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pipe::create::Output>>;

	fn close_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn delete_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_read_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pipe::Event>>>>>;

	fn write_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> BoxFuture<'a, tg::Result<()>>;
}

pub trait Pty: Send + Sync + 'static {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pty::create::Output>>;

	fn close_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn delete_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn get_pty_size<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::pty::Size>>>;

	fn try_read_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pty::Event>>>>>;

	fn write_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> BoxFuture<'a, tg::Result<()>>;
}

pub trait Remote: Send + Sync + 'static {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::remote::list::Output>>;

	fn try_get_remote<'a>(
		&'a self,
		name: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>>;

	fn put_remote<'a>(
		&'a self,
		name: &'a str,
		arg: tg::remote::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn delete_remote<'a>(&'a self, name: &'a str) -> BoxFuture<'a, tg::Result<()>>;
}

pub trait Tag: Send + Sync + 'static {
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::list::Output>>;

	fn try_get_tag<'a>(
		&'a self,
		pattern: &'a tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn put_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_tag_batch(&self, arg: tg::tag::post::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>>;
}

pub trait User: Send + Sync + 'static {
	fn get_user<'a>(&'a self, token: &'a str) -> BoxFuture<'a, tg::Result<Option<tg::User>>>;
}

pub trait Watch: Send + Sync + 'static {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::watch::list::Output>>;

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> BoxFuture<'_, tg::Result<()>>;
}

impl<T> Handle for T
where
	T: tg::handle::Handle,
{
	fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> BoxFuture<'_, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>> {
		self.cache(arg).map_ok(futures::StreamExt::boxed).boxed()
	}

	fn check(&self, arg: tg::check::Arg) -> BoxFuture<'_, tg::Result<tg::check::Output>> {
		self.check(arg).boxed()
	}

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkin::Output>>>>,
	> {
		self.checkin(arg).map_ok(futures::StreamExt::boxed).boxed()
	}

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::checkout::Output>>>>,
	> {
		self.checkout(arg).map_ok(futures::StreamExt::boxed).boxed()
	}

	fn clean(
		&self,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::clean::Output>>>>,
	> {
		self.clean().map_ok(futures::StreamExt::boxed).boxed()
	}

	fn document(&self, arg: tg::document::Arg) -> BoxFuture<'_, tg::Result<serde_json::Value>> {
		self.document(arg).boxed()
	}

	fn format(&self, arg: tg::format::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.format(arg).boxed()
	}

	fn health(&self) -> BoxFuture<'_, tg::Result<tg::Health>> {
		self.health().boxed()
	}

	fn index(
		&self,
	) -> BoxFuture<'_, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>> {
		self.index().map_ok(futures::StreamExt::boxed).boxed()
	}

	fn lsp<'a>(
		&'a self,
		input: BoxAsyncBufRead<'static>,
		output: BoxAsyncWrite<'static>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.lsp(input, output).boxed()
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::pull::Output>>>>,
	> {
		self.pull(arg).map_ok(futures::StreamExt::boxed).boxed()
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> BoxFuture<
		'_,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::push::Output>>>>,
	> {
		self.push(arg).map_ok(futures::StreamExt::boxed).boxed()
	}

	fn sync<'a>(
		&'a self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::sync::Message>>>> {
		self.sync(arg, stream)
			.map_ok(futures::StreamExt::boxed)
			.boxed()
	}

	fn try_get<'a>(
		&'a self,
		reference: &'a tg::Reference,
		arg: tg::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>>,
	> {
		self.try_get(reference, arg)
			.map_ok(futures::StreamExt::boxed)
			.boxed()
	}

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> BoxFuture<'_, tg::Result<Option<BoxStream<'static, tg::Result<tg::read::Event>>>>> {
		self.try_read_stream(arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn write<'a>(
		&'a self,
		reader: BoxAsyncRead<'static>,
	) -> BoxFuture<'a, tg::Result<tg::write::Output>> {
		self.write(reader).boxed()
	}
}

impl<T> Module for T
where
	T: tg::handle::Module,
{
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::resolve::Output>> {
		self.resolve_module(arg).boxed()
	}

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::load::Output>> {
		self.load_module(arg).boxed()
	}
}

impl<T> Object for T
where
	T: tg::handle::Object,
{
	fn try_get_object_metadata<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id, arg).boxed()
	}

	fn try_get_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id, arg).boxed()
	}

	fn put_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_object(id, arg).boxed()
	}

	fn post_object_batch(&self, arg: tg::object::batch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.post_object_batch(arg).boxed()
	}

	fn touch_object<'a>(
		&'a self,
		id: &'a tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.touch_object(id, arg).boxed()
	}
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

	fn try_get_process_log_stream<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::process::log::get::Event>>>>,
	> {
		self.try_get_process_log_stream(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
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

	fn cancel_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.cancel_process(id, arg).boxed()
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::process::dequeue::Output>>> {
		self.try_dequeue_process(arg).boxed()
	}

	fn finish_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.finish_process(id, arg).boxed()
	}

	fn heartbeat_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> BoxFuture<'a, tg::Result<tg::process::heartbeat::Output>> {
		self.heartbeat_process(id, arg).boxed()
	}

	fn post_process_log<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.post_process_log(id, arg).boxed()
	}

	fn signal_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.signal_process(id, arg).boxed()
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

	fn start_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::start::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.start_process(id, arg).boxed()
	}

	fn touch_process<'a>(
		&'a self,
		id: &'a tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.touch_process(id, arg).boxed()
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

impl<T> Pipe for T
where
	T: tg::handle::Pipe,
{
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pipe::create::Output>> {
		self.create_pipe(arg).boxed()
	}

	fn close_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.close_pipe(id, arg).boxed()
	}

	fn delete_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_pipe(id, arg).boxed()
	}

	fn try_read_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pipe::Event>>>>> {
		self.try_read_pipe(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn write_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pipe::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.write_pipe(id, arg, stream).boxed()
	}
}

impl<T> Pty for T
where
	T: tg::handle::Pty,
{
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pty::create::Output>> {
		self.create_pty(arg).boxed()
	}

	fn close_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.close_pty(id, arg).boxed()
	}

	fn delete_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_pty(id, arg).boxed()
	}

	fn get_pty_size<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::pty::Size>>> {
		self.get_pty_size(id, arg).boxed()
	}

	fn try_read_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pty::Event>>>>> {
		tg::handle::Pty::try_read_pty(self, id, arg)
			.map_ok(|opt| opt.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn write_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::pty::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.write_pty(id, arg, stream).boxed()
	}
}

impl<T> Remote for T
where
	T: tg::handle::Remote,
{
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::remote::list::Output>> {
		self.list_remotes(arg).boxed()
	}

	fn try_get_remote<'a>(
		&'a self,
		name: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(name).boxed()
	}

	fn put_remote<'a>(
		&'a self,
		name: &'a str,
		arg: tg::remote::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_remote(name, arg).boxed()
	}

	fn delete_remote<'a>(&'a self, name: &'a str) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_remote(name).boxed()
	}
}

impl<T> Tag for T
where
	T: tg::handle::Tag,
{
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::list::Output>> {
		self.list_tags(arg).boxed()
	}

	fn try_get_tag<'a>(
		&'a self,
		pattern: &'a tg::tag::Pattern,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(pattern, arg).boxed()
	}

	fn put_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_tag(tag, arg).boxed()
	}

	fn post_tag_batch(&self, arg: tg::tag::post::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.post_tag_batch(arg).boxed()
	}

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>> {
		self.delete_tag(arg).boxed()
	}
}

impl<T> User for T
where
	T: tg::handle::User,
{
	fn get_user<'a>(&'a self, token: &'a str) -> BoxFuture<'a, tg::Result<Option<tg::User>>> {
		self.get_user(token).boxed()
	}
}

impl<T> Watch for T
where
	T: tg::handle::Watch,
{
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::watch::list::Output>> {
		self.list_watches(arg).boxed()
	}

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.delete_watch(arg).boxed()
	}
}
