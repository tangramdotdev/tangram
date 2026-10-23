use {
	crate::{Server, Session},
	futures::future::BoxFuture,
	tangram_client::prelude::*,
};

pub use tangram_server_authentication_types::{Entry, Process, RemoteToken, Sandbox, cache, token};

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

impl Server {
	pub async fn authenticate(
		&self,
		origin: crate::Origin,
		token: Option<&str>,
	) -> tg::Result<Entry> {
		self.components
			.authentication
			.authenticate(self, origin, token)
			.await
	}

	pub async fn authenticate_with_claim(
		&self,
		context: &crate::Context,
		token: Option<&str>,
		process: Option<tg::process::Id>,
		sandbox: Option<tg::sandbox::Id>,
	) -> tg::Result<Entry> {
		self.components
			.authentication
			.authenticate_with_claim(self, context, token, process, sandbox)
			.await
	}

	pub fn create_process_authentication_token(&self, id: tg::process::Id) -> tg::Result<String> {
		self.components
			.authentication
			.create_process_authentication_token(self, id)
	}

	pub fn create_sandbox_authentication_token(&self, id: tg::sandbox::Id) -> tg::Result<String> {
		self.components
			.authentication
			.create_sandbox_authentication_token(self, id)
	}

	pub async fn invalidate_authentication(&self, invalidation: cache::Invalidation) -> () {
		self.components
			.authentication
			.invalidate_authentication(self, invalidation)
			.await;
	}

	pub async fn invalidate_user_authentication(&self, users: &[tg::user::Id]) -> () {
		self.components
			.authentication
			.invalidate_user_authentication(self, users)
			.await;
	}
}

impl Session {
	pub async fn try_get_authenticated_principal_remote_token(
		&self,
		remote: &str,
	) -> tg::Result<RemoteToken> {
		self.server
			.components
			.authentication
			.try_get_authenticated_principal_remote_token(self, remote)
			.await
	}

	pub async fn try_get_authenticated_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		self.server
			.components
			.authentication
			.try_get_authenticated_process(self, id)
			.await
	}
}
