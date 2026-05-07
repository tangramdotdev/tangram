use {
	crate::Server,
	futures::{Stream, stream::BoxStream},
	tangram_client::prelude::*,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod module;
mod object;
mod process;
mod remote;
mod sandbox;
mod tag;
mod user;
mod watch;

impl tg::Handle for Server {
	fn arg(&self) -> tg::Arg {
		self.arg()
	}

	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.session(&self.context).cache(arg).await
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.session(&self.context).check(arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.session(&self.context).checkin(arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.session(&self.context).checkout(arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.session(&self.context).clean().await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		self.session(&self.context).document(arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.session(&self.context).format(arg).await
	}

	async fn health(&self, arg: tg::health::Arg) -> tg::Result<tg::Health> {
		self.session(&self.context).health(arg).await
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.session(&self.context).index().await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.session(&self.context).lsp(input, output).await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		self.session(&self.context).pull(arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		self.session(&self.context).push(arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		self.session(&self.context).sync(arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.session(&self.context).try_get(reference, arg).await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.session(&self.context).try_read_stream(arg).await
	}

	async fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.session(&self.context).write(arg, reader).await
	}
}
