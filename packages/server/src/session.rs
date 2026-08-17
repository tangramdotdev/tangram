use {
	crate::{Context, Server},
	futures::{Stream, stream::BoxStream},
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod checkpoint;
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
pub(crate) struct Session {
	pub server: Server,
	pub context: Context,
}

impl Session {
	#[must_use]
	pub(crate) fn new(server: Server, context: Context) -> Self {
		Self { server, context }
	}

	pub(crate) fn verify_request_from_host(&self) -> tg::Result<()> {
		if self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
			.is_some()
		{
			return Err(tg::error!("the operation is not available from a sandbox"));
		}

		Ok(())
	}

	pub(crate) fn verify_request_can_access_remote_process(&self) -> tg::Result<()> {
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

	pub(crate) fn verify_request_with_network_access(&self) -> tg::Result<()> {
		if !self.server.origin_has_network_access(self.context.origin)? {
			return Err(tg::error!(
				"network access is disabled for the origin sandbox"
			));
		}

		Ok(())
	}

	pub(crate) fn host_path_for_guest_path(&self, path: &Path) -> tg::Result<PathBuf> {
		let Some(sandbox) = self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
		else {
			return Ok(path.to_owned());
		};
		let id = sandbox.data.id.clone();
		let sandbox = sandbox
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!(%id, "failed to get the origin sandbox"))?;

		// Resolve a guest store path to the checkouts directory, where the shared artifacts live.
		if self.server.vfs.lock().unwrap().is_some()
			&& let Ok(rest) = path.strip_prefix(sandbox.guest_store_path())
		{
			return Ok(self.server.checkout_path().join(rest));
		}

		sandbox
			.host_path_for_guest_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no host path for guest path"))
	}

	pub(crate) fn guest_path_for_host_path(&self, path: &Path) -> tg::Result<PathBuf> {
		let Some(sandbox) = self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
		else {
			return Ok(path.to_owned());
		};
		let id = sandbox.data.id.clone();
		let sandbox = sandbox
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!(%id, "failed to get the origin sandbox"))?;

		// Serve a shared store path through the per-sandbox VFS mount.
		if self.server.vfs.lock().unwrap().is_some()
			&& let Ok(rest) = path.strip_prefix(self.server.store_path())
		{
			return Ok(sandbox.guest_store_path().join(rest));
		}

		sandbox
			.guest_path_for_host_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no guest path for host path"))
	}
}

impl tg::Handle for Session {
	fn arg(&self) -> tg::Arg {
		self.server.arg()
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

	async fn match_(&self, arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		self.match_(arg).await
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

	async fn try_resolve(
		&self,
		reference: &tg::Reference,
		arg: tg::resolve::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::resolve::Output>>>>
		+ Send
		+ 'static,
	> {
		self.try_resolve(reference, arg).await
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
