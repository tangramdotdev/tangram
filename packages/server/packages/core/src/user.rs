use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The user component.
pub trait Component: crate::component::Lifecycle {
	fn create_user_token<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::token::create::Output>>;

	fn try_delete_user_token<'a>(
		&'a self,
		session: &'a Session,
		token: &'a tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_user_tokens<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::token::list::Output>>;

	fn get_current_user<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::current::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::get::Output>>>;

	fn create_login<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::login::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::login::create::Output>>;

	fn logout<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::logout::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_user<'a>(
		&'a self,
		session: &'a Session,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::get::Output>>>;

	fn try_get_user_usage<'a>(
		&'a self,
		session: &'a Session,
		user: Option<&'a tg::user::Selector>,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>>;

	fn manage_user_billing<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::billing::manage::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::billing::manage::Output>>;

	fn wait_login<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::user::login::wait::Arg,
	) -> BoxFuture<'a, tg::Result<tg::user::login::wait::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn try_get_user_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::user::Id,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::user::Data>, crate::database::Error>>>;
}
