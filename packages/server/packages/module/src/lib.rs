use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

pub mod load;
pub mod resolve;

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
			(http::Method::POST, ["modules", "load"]) => {
				self.load_module_request(session, request).await
			},
			(http::Method::POST, ["modules", "resolve"]) => {
				self.resolve_module_request(session, request).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::module::Component for Component {
	fn resolve_module<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::module::resolve::Arg,
	) -> BoxFuture<'a, tg::Result<tg::module::resolve::Output>> {
		self.resolve_module(session, arg).boxed()
	}

	fn load_module<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::module::load::Arg,
	) -> BoxFuture<'a, tg::Result<tg::module::load::Output>> {
		self.load_module(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
