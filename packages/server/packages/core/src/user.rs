use {
	crate::Session,
	futures::future::BoxFuture,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
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
}

pub async fn try_get_user_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	id: &tg::user::Id,
) -> tg::Result<ControlFlow<Option<tg::user::Data>, crate::database::Error>> {
	#[derive(db::row::Deserialize)]
	struct EmailRow {
		email: String,
	}

	#[derive(db::row::Deserialize)]
	struct UserRow {
		name: String,
	}

	let specifier = match crate::Session::try_get_specifier_for_id_with_transaction(
		transaction,
		&id.clone().into(),
	)
	.await?
	{
		ControlFlow::Break(specifier) => specifier,
		ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
	};
	let Some(specifier) = specifier else {
		return Ok(ControlFlow::Break(None));
	};
	let p = transaction.p();
	let statement = formatdoc!(
		"
			select name
			from users
			where id = {p}1;
		"
	);
	let result = transaction
		.query_optional_into::<UserRow>(statement.into(), db::params![id.to_string()])
		.await;
	let user = crate::database::retry!(result, "failed to execute the statement");
	let Some(user) = user else {
		return Ok(ControlFlow::Break(None));
	};
	let statement = formatdoc!(
		r#"
			select email
			from user_emails
			where "user" = {p}1
			order by email;
		"#
	);
	let result = transaction
		.query_all_into::<EmailRow>(statement.into(), db::params![id.to_string()])
		.await;
	let rows = crate::database::retry!(result, "failed to execute the statement");
	let user = tg::user::Data {
		emails: rows.into_iter().map(|row| row.email).collect(),
		id: id.clone(),
		name: user.name,
		specifier,
	};

	Ok(ControlFlow::Break(Some(user)))
}
