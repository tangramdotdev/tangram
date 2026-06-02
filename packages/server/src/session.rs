use {
	crate::{Context, Server},
	futures::{Stream, future, stream, stream::BoxStream},
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

	#[must_use]
	pub(crate) fn read_principal(&self) -> tg::Principal {
		// match self.context.authentication.as_ref() {
		// 	None | Some(Authentication::Runner) => tg::Principal::All,
		// 	Some(Authentication::Process(process)) => process
		// 		.created_by
		// 		.clone()
		// 		.map_or(tg::Principal::Root, tg::Principal::User),
		// 	Some(Authentication::Root) => tg::Principal::Root,
		// 	Some(Authentication::Sandbox(sandbox)) => sandbox
		// 		.created_by
		// 		.clone()
		// 		.map_or(tg::Principal::Root, tg::Principal::User),
		// 	Some(Authentication::User(user)) => tg::Principal::User(user.id.clone()),
		// }
		Err::<tg::Principal, _>(tg::error!("todo")).unwrap()
	}

	#[must_use]
	pub(crate) fn write_principal(&self) -> Option<tg::Principal> {
		// match self.context.authentication.as_ref() {
		// 	Some(Authentication::User(user)) => Some(tg::Principal::User(user.id.clone())),
		// 	Some(Authentication::Process(process)) => {
		// 		process.created_by.clone().map(tg::Principal::User)
		// 	},
		// 	Some(Authentication::Sandbox(sandbox)) => {
		// 		sandbox.created_by.clone().map(tg::Principal::User)
		// 	},
		// 	None | Some(Authentication::Root | Authentication::Runner) => None,
		// }
		Err::<Option<tg::Principal>, _>(tg::error!("todo")).unwrap()
	}

	// #[must_use]
	// pub(crate) fn authorize_object(
	// 	principal: &tg::Principal,
	// 	output: &crate::object::store::TryGetOutput,
	// 	subtree: bool,
	// ) -> bool {
	// 	// matches!(principal, tg::Principal::Root)
	// 	// 	|| if subtree {
	// 	// 		output.grants.iter().any(|grant| grant.subtree)
	// 	// 	} else {
	// 	// 		!output.grants.is_empty()
	// 	// 	}
	// 	Err::<bool, _>(tg::error!("todo")).unwrap()
	// }

	pub(crate) fn host_path_for_guest_path(&self, path: &Path) -> tg::Result<PathBuf> {
		// let Some(sandbox) = self
		// 	.context
		// 	.authentication
		// 	.as_ref()
		// 	.and_then(|authentication| authentication.try_unwrap_process_ref().ok())
		// 	.map(|process| &process.sandbox)
		// else {
		// 	return Ok(path.to_owned());
		// };
		// let sandbox = self
		// 	.server
		// 	.sandboxes
		// 	.get(sandbox)
		// 	.map(|sandbox| sandbox.value().clone())
		// 	.ok_or_else(|| tg::error!(%sandbox, "failed to get the sandbox"))?;
		// sandbox
		// 	.host_path_for_guest_path(path)
		// 	.ok_or_else(|| tg::error!(path = %path.display(), "no host path for guest path"))
		Err(tg::error!("todo"))
	}

	pub(crate) fn guest_path_for_host_path(&self, path: &Path) -> tg::Result<PathBuf> {
		// let Some(sandbox) = self
		// 	.context
		// 	.authentication
		// 	.as_ref()
		// 	.and_then(|authentication| authentication.try_unwrap_process_ref().ok())
		// 	.map(|process| &process.sandbox)
		// else {
		// 	return Ok(path.to_owned());
		// };
		// let sandbox = self
		// 	.server
		// 	.sandboxes
		// 	.get(sandbox)
		// 	.map(|sandbox| sandbox.value().clone())
		// 	.ok_or_else(|| tg::error!(%sandbox, "failed to get the sandbox"))?;
		// sandbox
		// 	.guest_path_for_host_path(path)
		// 	.ok_or_else(|| tg::error!(path = %path.display(), "no guest path for host path"))
		Err(tg::error!("todo"))
	}
}

impl tg::Handle for Session {
	fn arg(&self) -> tg::Arg {
		// self.server.arg()
		Err::<tg::Arg, _>(tg::error!("todo")).unwrap()
	}

	async fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		// self.cache(arg).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn check(&self, arg: tg::check::Arg) -> tg::Result<tg::check::Output> {
		// self.check(arg).await
		Err(tg::error!("todo"))
	}

	async fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
	> {
		// self.checkin(arg).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
	> {
		// self.checkout(arg).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
		// self.clean().await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn document(&self, arg: tg::document::Arg) -> tg::Result<serde_json::Value> {
		// self.document(arg).await
		Err(tg::error!("todo"))
	}

	async fn format(&self, arg: tg::format::Arg) -> tg::Result<()> {
		// self.format(arg).await
		Err(tg::error!("todo"))
	}

	async fn health(&self, arg: tg::health::Arg) -> tg::Result<tg::Health> {
		// self.health(arg).await
		Err(tg::error!("todo"))
	}

	async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		// self.index().await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		// self.list(arg).await
		Err(tg::error!("todo"))
	}

	async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		// self.lsp(input, output).await
		Err(tg::error!("todo"))
	}

	async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
	> {
		// self.pull(arg).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
	> {
		// self.push(arg).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static> {
		// self.sync(arg, stream).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>> + Send + 'static,
	> {
		// self.try_get(reference, arg).await
		Ok(stream::once(future::ready(Err(tg::error!("todo")))))
	}

	async fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>> {
		// self.try_read_stream(arg).await
		Ok(Some(stream::once(future::ready(Err(tg::error!("todo"))))))
	}

	async fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<tg::write::Output> {
		// self.write(arg, reader).await
		Err(tg::error!("todo"))
	}
}
