use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
	tangram_futures::{BoxAsyncBufRead, BoxAsyncRead, BoxAsyncWrite},
};

mod module;
mod object;
mod pipe;
mod process;
mod pty;
mod remote;
mod tag;
mod user;
mod watch;

pub use self::{
	module::Module, object::Object, pipe::Pipe, process::Process, pty::Pty, remote::Remote,
	tag::Tag, user::User, watch::Watch,
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
		arg: tg::write::Arg,
		reader: BoxAsyncRead<'static>,
	) -> BoxFuture<'a, tg::Result<tg::write::Output>>;
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
		arg: tg::write::Arg,
		reader: BoxAsyncRead<'static>,
	) -> BoxFuture<'a, tg::Result<tg::write::Output>> {
		self.write(arg, reader).boxed()
	}
}
