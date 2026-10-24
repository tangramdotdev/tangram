use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The remote component.
pub trait Component: crate::component::Lifecycle {
	fn list_remotes<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::remote::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::remote::list::Output>>;

	fn put_remote<'a>(
		&'a self,
		session: &'a Session,
		name: &'a str,
		arg: tg::remote::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_remote<'a>(
		&'a self,
		session: &'a Session,
		name: &'a str,
		arg: tg::remote::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>>;

	fn try_get_remote_for_principal<'a>(
		&'a self,
		session: &'a Session,
		name: &'a str,
		principal: Option<tg::principal::Selector>,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>>;

	fn try_delete_remote<'a>(
		&'a self,
		session: &'a Session,
		name: &'a str,
		arg: tg::remote::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn verify_request_can_mutate_remotes(&self, session: &Session) -> tg::Result<()>;

	fn try_resolve_remote_context_principal<'a>(
		&'a self,
		session: &'a Session,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<Option<tg::Principal>>>;

	fn get_remote_session<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<tg::Session>>;

	fn get_remote_session_for_process<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<tg::Session>>;

	fn try_get_remote_session<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::Session>>>;

	fn set_remote_token<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
		token: Option<String>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_cached_remote_response<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
		request: &'a tangram_server_remote_core::cache::Request,
		ttl: tg::remote::cache::Ttl,
	) -> BoxFuture<'a, tg::Result<Option<tangram_server_remote_core::cache::Response>>>;

	fn put_cached_remote_response<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
		request: &'a tangram_server_remote_core::cache::Request,
		response: &'a tangram_server_remote_core::cache::Response,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn invalidate_remote_cache<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
	) -> BoxFuture<'a, ()>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
