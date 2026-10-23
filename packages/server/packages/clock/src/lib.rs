use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

mod advance;
mod get;
mod set;

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
			(http::Method::GET, ["clock"]) => self.get_clock_request(session, request).await,
			(http::Method::POST, ["clock", "advance"]) => {
				self.advance_clock_request(session, request).await
			},
			(http::Method::POST, ["clock", "set"]) => {
				self.set_clock_request(session, request).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}

	fn virtual_clock(
		session: &Session,
	) -> tg::Result<Option<&std::sync::Arc<tangram_server_core::clock::Virtual>>> {
		session.verify_request_from_host()?;
		if !matches!(session.context.principal, tg::Principal::Root) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(session.server.clock.try_virtual())
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::clock::Component for Component {
	fn try_advance_clock<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::clock::advance::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::clock::get::Output>>> {
		self.try_advance_clock(session, arg).boxed()
	}

	fn try_get_clock<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
	) -> BoxFuture<'a, tg::Result<Option<tg::clock::get::Output>>> {
		self.try_get_clock(session).boxed()
	}

	fn try_set_clock<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::clock::set::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::clock::get::Output>>> {
		self.try_set_clock(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
