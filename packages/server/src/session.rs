use {
	crate::{Context, Server},
	futures::{Stream, stream::BoxStream},
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod group;
mod module;
mod namespace;
mod object;
mod process;
mod remote;
mod sandbox;
mod tag;
mod user;
mod watch;

#[derive(Clone)]
pub(crate) struct Session {
	pub server: Server,
	pub context: Context,
}

impl Session {
	#[must_use]
	pub(crate) fn new(server: Server, context: Context) -> Self {
		Self { server, context }
	}

	pub(crate) async fn get_region_session(&self, region: String) -> tg::Result<tg::Session> {
		let _token = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_process_ref().ok())
			.map(|process| process.token.clone());
		let _client = self.server.get_region_client(region).await?;
		todo!("propagate process authentication to regions")
	}

	pub(crate) fn host_path_for_guest_path(&self, path: &Path) -> tg::Result<PathBuf> {
		let Some(sandbox) = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_process_ref().ok())
			.map(|process| &process.sandbox)
		else {
			return Ok(path.to_owned());
		};
		let sandbox = self
			.server
			.sandboxes
			.get(sandbox)
			.map(|sandbox| sandbox.value().clone())
			.ok_or_else(|| tg::error!(%sandbox, "failed to get the sandbox"))?;
		sandbox
			.host_path_for_guest_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no host path for guest path"))
	}

	pub(crate) fn guest_path_for_host_path(&self, path: &Path) -> tg::Result<PathBuf> {
		let Some(sandbox) = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_process_ref().ok())
			.map(|process| &process.sandbox)
		else {
			return Ok(path.to_owned());
		};
		let sandbox = self
			.server
			.sandboxes
			.get(sandbox)
			.map(|sandbox| sandbox.value().clone())
			.ok_or_else(|| tg::error!(%sandbox, "failed to get the sandbox"))?;
		sandbox
			.guest_path_for_host_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no guest path for host path"))
	}
}

impl tg::Handle for Session {
	fn arg(&self) -> tg::Arg {
		self.server.arg()
	}

	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.cache(arg).await
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		self.check(arg).await
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		self.checkin(arg).await
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		self.checkout(arg).await
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		self.clean().await
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		self.document(arg).await
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		self.format(arg).await
	}

	async fn health(&self, arg: tg::health::Arg) -> tg::Result<tg::Health> {
		self.health(arg).await
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		self.index().await
	}

	async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.list(arg).await
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		self.lsp(input, output).await
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		self.pull(arg).await
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		self.push(arg).await
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		self.sync(arg, stream).await
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		self.try_get(reference, arg).await
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		self.try_read_stream(arg).await
	}

	async fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		self.write(arg, reader).await
	}
}
