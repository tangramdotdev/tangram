use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
	std::sync::Arc,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
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

#[derive(Clone)]
pub struct Handle(Arc<dyn super::erased::Handle>);

impl Handle {
	pub fn new(handle: impl tg::Handle) -> Self {
		Self(Arc::new(handle))
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
	> {
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
	> {
		self.0.index()
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.0.lsp(Box::pin(input), Box::pin(output))
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
	> {
		unsafe {
			std::mem::transmute::<_, futures::future::BoxFuture<'_, tg::Result<BoxStream<_>>>>(
				self.0.try_get(reference, arg),
			)
		}
	}

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, futures::future::BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_read_stream(arg),
			)
		}
	}

	fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::write::Output>> {
		self.0.write(arg, Box::pin(reader))
	}
}
