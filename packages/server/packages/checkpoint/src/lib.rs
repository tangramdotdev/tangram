use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

mod abort;
mod continue_;
mod panic;
mod unwatch;
mod wait;
mod watch;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::DELETE, ["checkpoints", checkpoint, "watches", watch]) => {
				self.unwatch_checkpoint_request(session, request, checkpoint, watch)
					.await
			},
			(http::Method::POST, ["checkpoints", checkpoint, "abort"]) => {
				self.abort_checkpoint_request(session, request, checkpoint)
					.await
			},
			(http::Method::POST, ["checkpoints", checkpoint, "panic"]) => {
				self.panic_checkpoint_request(session, request, checkpoint)
					.await
			},
			(http::Method::POST, ["checkpoints", checkpoint, "watches"]) => {
				self.watch_checkpoint_request(session, request, checkpoint)
					.await
			},
			(
				http::Method::POST,
				[
					"checkpoints",
					checkpoint,
					"watches",
					watch,
					"hits",
					hit,
					"continue",
				],
			) => {
				self.continue_checkpoint_hit_request(session, request, checkpoint, watch, hit)
					.await
			},
			(
				http::Method::POST,
				[
					"checkpoints",
					checkpoint,
					"watches",
					watch,
					"hits",
					hit,
					"wait",
				],
			) => {
				self.wait_checkpoint_hit_request(session, request, checkpoint, watch, hit)
					.await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}

	fn checkpoint_state(
		session: &Session,
	) -> tg::Result<Option<&tangram_server_core::checkpoint::State>> {
		session.verify_request_from_host()?;
		if !matches!(session.context.principal, tg::Principal::Root) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(session.server.checkpoints.as_deref())
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::checkpoint::Component for Component {
	fn try_abort_checkpoint<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		checkpoint: &'a str,
		arg: tg::checkpoint::abort::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_abort_checkpoint(session, checkpoint, arg).boxed()
	}

	fn try_continue_checkpoint_hit<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_continue_checkpoint_hit(session, checkpoint, watch, hit)
			.boxed()
	}

	fn try_panic_checkpoint<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		checkpoint: &'a str,
		arg: tg::checkpoint::panic::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_panic_checkpoint(session, checkpoint, arg).boxed()
	}

	fn try_unwatch_checkpoint<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		checkpoint: &'a str,
		watch: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_unwatch_checkpoint(session, checkpoint, watch)
			.boxed()
	}

	fn try_wait_checkpoint_hit<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::wait::Output>>> {
		self.try_wait_checkpoint_hit(session, checkpoint, watch, hit)
			.boxed()
	}

	fn try_watch_checkpoint<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		checkpoint: &'a str,
		arg: tg::checkpoint::watch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::watch::Output>>> {
		self.try_watch_checkpoint(session, checkpoint, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
