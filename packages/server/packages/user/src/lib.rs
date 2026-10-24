use {
	futures::{FutureExt as _, future::BoxFuture},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

pub mod billing;
pub mod current;
pub mod get;
pub mod login;
pub mod logout;
pub mod oauth;
pub mod token;
pub mod usage;

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
			(http::Method::POST, ["logins"]) => self.create_login_request(session, request).await,
			(http::Method::POST, ["login", "wait"]) => {
				self.wait_login_request(session, request).await
			},
			(http::Method::GET, ["user", "usage"]) => {
				self.try_get_user_usage_request(session, request, None)
					.await
			},
			(http::Method::POST, ["logout"]) => self.logout_request(session, request).await,
			(http::Method::GET, ["oauth", "github", "authorize"]) => {
				self.oauth_github_authorize_request(session, request).await
			},
			(http::Method::GET, ["oauth", "github", "callback"]) => {
				self.oauth_github_callback_request(session, request).await
			},
			(http::Method::GET, ["users", user]) => {
				self.try_get_user_request(session, request, user).await
			},
			(http::Method::GET, ["users", user, "usage"]) => {
				self.try_get_user_usage_request(session, request, Some(user))
					.await
			},
			(http::Method::GET, ["user"]) => self.get_current_user_request(session, request).await,
			(http::Method::POST, ["user", "billing", "manage"]) => {
				self.manage_user_billing_request(session, request).await
			},
			(http::Method::POST, ["user", "tokens"]) => {
				self.create_user_token_request(session, request).await
			},
			(http::Method::GET, ["user", "tokens"]) => {
				self.list_user_tokens_request(session, request).await
			},
			(http::Method::DELETE, ["user", "tokens", token]) => {
				self.try_delete_user_token_request(session, request, token)
					.await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::user::Component for Component {
	fn create_user_token<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::token::create::Output>> {
		self.create_user_token(session, arg).boxed()
	}

	fn try_delete_user_token<'a>(
		&'a self,
		session: &'a Session,
		token: &'a tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_user_token(session, token, arg).boxed()
	}

	fn list_user_tokens<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::token::list::Output>> {
		self.list_user_tokens(session, arg).boxed()
	}

	fn get_current_user<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::current::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::get::Output>>> {
		self.get_current_user(session, arg).boxed()
	}

	fn create_login<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::login::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::login::create::Output>> {
		self.create_login(session, arg).boxed()
	}

	fn logout<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::logout::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.logout(session, arg).boxed()
	}

	fn try_get_user<'a>(
		&'a self,
		session: &'a Session,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::get::Output>>> {
		self.try_get_user(session, user, arg).boxed()
	}

	fn try_get_user_usage<'a>(
		&'a self,
		session: &'a Session,
		user: Option<&'a tg::user::Selector>,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>> {
		self.try_get_user_usage(session, user, arg).boxed()
	}

	fn manage_user_billing<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::billing::manage::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::billing::manage::Output>> {
		self.manage_user_billing(session, arg).boxed()
	}

	fn wait_login<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::login::wait::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::login::wait::Output>> {
		self.wait_login(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}

	fn try_get_user_with_transaction<'a>(
		&'a self,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		id: &'a tg::user::Id,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::user::Data>, tangram_server_core::database::Error>>,
	> {
		Self::try_get_user_with_transaction(transaction, id).boxed()
	}
}
