use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod grant;
mod group;
mod module;
mod object;
mod organization;
mod process;
mod remote;
mod runner;
mod sandbox;
mod tag;
mod user;
mod watch;

#[derive(Clone, Debug)]
pub struct Session {
	client: tg::Client,
	context: tg::Context,
}

impl Session {
	#[must_use]
	pub fn new(client: tg::Client, context: tg::Context) -> Self {
		Self { client, context }
	}

	#[must_use]
	pub fn arg(&self) -> tg::Arg {
		tg::Arg {
			url: Some(self.client.url().clone()),
			version: Some(self.client.version().to_owned()),
			token: self.context.token.clone(),
			pool: Some(self.client.pool_options),
			reconnect: Some(self.client.reconnect.clone()),
			retry: Some(self.client.retry.clone()),
			sync: self.client.sync,
		}
	}

	#[must_use]
	pub fn client(&self) -> &tg::Client {
		&self.client
	}

	#[must_use]
	pub fn context(&self) -> &tg::Context {
		&self.context
	}
}

impl tg::Handle for tg::Session {
	fn arg(&self) -> tg::Arg {
		self.arg()
	}

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
	> {
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

	fn health(&self, arg: tg::health::Arg) -> impl Future<Output = tg::Result<tg::Health>> {
		self.health(arg)
	}

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		self.index()
	}

	fn list(&self, arg: tg::list::Arg) -> impl Future<Output = tg::Result<tg::list::Output>> {
		self.list(arg)
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
	> {
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
		self.try_read_blob_stream(arg)
	}

	fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::write::Output>> {
		self.write(arg, reader)
	}
}
