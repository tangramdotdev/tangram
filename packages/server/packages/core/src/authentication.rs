use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	tangram_client::prelude::*,
};

pub use tangram_server_authentication_core::{Entry, Process, RemoteToken, cache};

/// The authentication component.
pub trait Component: crate::component::Lifecycle {
	fn try_get_authenticated_principal_remote_token<'a>(
		&'a self,
		session: &'a Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<crate::authentication::RemoteToken>>;

	fn try_get_authenticated_process<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
	) -> BoxFuture<'a, tg::Result<Option<crate::authentication::Process>>>;

	fn authenticate<'a>(
		&'a self,
		server: &'a Server,
		origin: crate::Origin,
		token: Option<&'a str>,
	) -> BoxFuture<'a, tg::Result<crate::authentication::Entry>>;

	fn authenticate_with_claim<'a>(
		&'a self,
		server: &'a Server,
		context: &'a crate::Context,
		token: Option<&'a str>,
		process: Option<tg::process::Id>,
		sandbox: Option<tg::sandbox::Id>,
	) -> BoxFuture<'a, tg::Result<crate::authentication::Entry>>;

	fn create_process_authentication_token<'a>(
		&'a self,
		server: &'a Server,
		id: tg::process::Id,
	) -> tg::Result<String>;

	fn create_sandbox_authentication_token<'a>(
		&'a self,
		server: &'a Server,
		id: tg::sandbox::Id,
	) -> tg::Result<String>;

	fn invalidate_authentication<'a>(
		&'a self,
		server: &'a Server,
		invalidation: crate::authentication::cache::Invalidation,
	) -> BoxFuture<'a, ()>;

	fn invalidate_user_authentication<'a>(
		&'a self,
		server: &'a Server,
		users: &'a [tg::user::Id],
	) -> BoxFuture<'a, ()>;
}
