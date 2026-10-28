use {
	crate::{Context, Server},
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

#[derive(Clone)]
pub struct Session {
	pub context: Context,
	pub server: Server,
}

impl Session {
	#[must_use]
	pub fn new(server: Server, context: Context) -> Self {
		Self { context, server }
	}

	pub fn verify_request_from_host(&self) -> tg::Result<()> {
		if self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
			.is_some()
		{
			return Err(tg::error!("the operation is not available from a sandbox"));
		}

		Ok(())
	}

	pub fn verify_request_can_access_remote_process(&self) -> tg::Result<()> {
		if self.server.origin_has_network_access(self.context.origin)?
			|| matches!(self.context.origin, crate::Origin::Sandbox(_))
				&& matches!(
					self.context.principal,
					tg::Principal::Process(_)
						| tg::Principal::Runner(_)
						| tg::Principal::Sandbox(_)
				) {
			return Ok(());
		}

		Err(tg::error!(
			"network access is disabled for the origin sandbox"
		))
	}

	pub fn verify_request_with_network_access(&self) -> tg::Result<()> {
		if !self.server.origin_has_network_access(self.context.origin)? {
			return Err(tg::error!(
				"network access is disabled for the origin sandbox"
			));
		}

		Ok(())
	}
}

impl tg::Instance for Session {
	fn arg(&self) -> tg::Arg {
		self.server.arg()
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.server.components.check.check(self, arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.server.components.checkin.checkin(self, arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.server.components.checkout.checkout(self, arg).await
	}

	async fn children(&self, arg: tg::children::Arg) -> tg::Result<tg::children::Output> {
		self.server.components.children.children(self, arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.server.components.clean.clean(self).await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<tg::document::Output> {
		self.server.components.document.document(self, arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.server.components.format.format(self, arg).await
	}

	async fn health(&self, arg: tg::health::Arg) -> tg::Result<tg::Health> {
		self.server.components.health.health(self, arg).await
	}

	async fn index(
		&self,
		arg: tg::index::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.server.components.indexer.index(self, arg).await
	}

	async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.server.components.list.list(self, arg).await
	}

	async fn match_(&self, arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		self.server.components.match_.match_(self, arg).await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.server
			.components
			.lsp
			.lsp(self, Box::new(input), Box::new(output))
			.await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<(
		tg::pull::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	)> {
		self.server.components.pull.pull(self, arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<(
		tg::push::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	)> {
		self.server.components.push.push(self, arg).await
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
		self.server.components.sync.sync(self, arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.server
			.components
			.get
			.try_get(self, reference, arg)
			.await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.server.components.read.try_read_stream(self, arg).await
	}

	async fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.server
			.components
			.write
			.write(self, arg, Box::pin(reader))
			.await
	}
}
