use {
	dashmap::DashMap,
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
	tangram_server_remote_core::cache::{Request, Response},
	tangram_uri::Uri,
};

mod cache;
mod client;
mod delete;
mod get;
mod list;
mod principal;
mod put;
mod session;
mod token;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub clients: DashMap<Uri, tg::Client, fnv::FnvBuildHasher>,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		let clients = DashMap::default();
		let state = State { clients };
		Self(Arc::new(state))
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
			(http::Method::DELETE, ["remotes", name]) => {
				self.delete_remote_request(session, request, name).await
			},
			(http::Method::GET, ["remotes"]) => self.list_remotes_request(session, request).await,
			(http::Method::GET, ["remotes", name]) => {
				self.try_get_remote_request(session, request, name).await
			},
			(http::Method::PUT, ["remotes", name]) => {
				self.put_remote_request(session, request, name).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl Default for Component {
	fn default() -> Self {
		Self::new()
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::remote::Component for Component {
	fn list_remotes<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::remote::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::remote::list::Output>> {
		self.list_remotes(session, arg).boxed()
	}

	fn put_remote<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		name: &'a str,
		arg: tg::remote::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_remote(session, name, arg).boxed()
	}

	fn try_get_remote<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		name: &'a str,
		arg: tg::remote::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(session, name, arg).boxed()
	}

	fn try_get_remote_for_principal<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		name: &'a str,
		principal: Option<tg::principal::Selector>,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote_for_principal(session, name, principal)
			.boxed()
	}

	fn try_delete_remote<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		name: &'a str,
		arg: tg::remote::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_remote(session, name, arg).boxed()
	}

	fn verify_request_can_mutate_remotes(
		&self,
		session: &tangram_server_core::Session,
	) -> tg::Result<()> {
		Self::verify_request_can_mutate_remotes(session)
	}

	fn try_resolve_remote_context_principal<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<Option<tg::Principal>>> {
		self.try_resolve_remote_context_principal(session, principal)
			.boxed()
	}

	fn get_remote_session<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<tg::Session>> {
		self.get_remote_session(session, remote).boxed()
	}

	fn get_remote_session_for_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<tg::Session>> {
		self.get_remote_session_for_process(session, remote).boxed()
	}

	fn try_get_remote_session<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::Session>>> {
		self.try_get_remote_session(session, remote).boxed()
	}

	fn set_remote_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
		token: Option<String>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.set_remote_token(session, remote, token).boxed()
	}

	fn try_get_cached_remote_response<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
		request: &'a Request,
		ttl: tg::remote::cache::Ttl,
	) -> BoxFuture<'a, tg::Result<Option<Response>>> {
		self.try_get_cached_remote_response(session, remote, request, ttl)
			.boxed()
	}

	fn put_cached_remote_response<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
		request: &'a Request,
		response: &'a Response,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_cached_remote_response(session, remote, request, response)
			.boxed()
	}

	fn invalidate_remote_cache<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
	) -> BoxFuture<'a, ()> {
		self.invalidate_remote_cache(session, remote).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
