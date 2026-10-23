use {
	crate::Server,
	futures::{Stream, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod checkpoint;
mod clock;
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

impl tg::Instance for Server {
	fn arg(&self) -> tg::Arg {
		self.arg()
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
		let session = self.session(&self.context);
		self.components.checkin.checkin(&session, arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		let session = self.session(&self.context);
		self.components.checkout.checkout(&session, arg).await
	}

	async fn children(&self, arg: tg::children::Arg) -> tg::Result<tg::children::Output> {
		self.session(&self.context).children(arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		let session = self.session(&self.context);
		self.components.clean.clean(&session).await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<tg::document::Output> {
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

	async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.session(&self.context).list(arg).await
	}

	async fn match_(&self, arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		self.session(&self.context).match_(arg).await
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
	) -> tg::Result<(
		tg::pull::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	)> {
		let session = self.session(&self.context);
		self.components.pull.pull(&session, arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<(
		tg::push::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	)> {
		let session = self.session(&self.context);
		self.components.push.push(&session, arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sync::Header,
				impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static,
			),
			tg::Error,
		>,
	> {
		let session = self.session(&self.context);
		self.components.sync.sync(&session, arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		let session = self.session(&self.context);
		self.components.get.try_get(&session, reference, arg).await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		let session = self.session(&self.context);
		self.components.read.try_read_stream(&session, arg).await
	}

	async fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.session(&self.context).write(arg, reader).await
	}
}
