use {
	crate::Session,
	futures::future::BoxFuture,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::body::Boxed as BoxBody,
};

/// The organization component.
pub trait Component: crate::component::Lifecycle {
	fn create_organization<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::organization::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::create::Output>>;

	fn try_get_organization<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::organization::get::Output>>>;

	fn try_get_organization_usage<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>>;

	fn try_delete_organization<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_organization_members<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::members::list::Output>>;

	fn add_organization_member<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_organization_member<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn manage_organization_billing<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::billing::manage::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::billing::manage::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}

pub async fn try_get_organization_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	id: &tg::organization::Id,
) -> tg::Result<ControlFlow<Option<tg::organization::Data>, crate::database::Error>> {
	#[derive(db::row::Deserialize)]
	struct Row {
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
			from organizations
			where id = {p}1;
		"
	);
	let result = transaction
		.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
		.await;
	let row = crate::database::retry!(result, "failed to execute the statement");
	let organization = row.map(|row| tg::organization::Data {
		id: id.clone(),
		name: row.name,
		specifier,
	});

	Ok(ControlFlow::Break(organization))
}
