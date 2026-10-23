use {
	crate::Session,
	futures::future::BoxFuture,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::body::Boxed as BoxBody,
};

/// The group component.
pub trait Component: crate::component::Lifecycle {
	fn create_group<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::group::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::create::Output>>;

	fn try_get_group<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::group::get::Output>>>;

	fn try_delete_group<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_group_members<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::members::list::Output>>;

	fn add_group_member<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_group_member<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		member: &'a tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn create_parent_groups_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		specifier: &'a tg::Specifier,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>>>;
}

pub async fn try_get_group_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	id: &tg::group::Id,
) -> tg::Result<ControlFlow<Option<tg::group::Data>, crate::database::Error>> {
	#[derive(db::row::Deserialize)]
	struct Row {
		name: String,
		#[tangram_database(as = "Option<db::value::FromStr>")]
		parent: Option<tg::Id>,
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
			select name, parent
			from groups
			where id = {p}1;
		"
	);
	let result = transaction
		.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
		.await;
	let row = crate::database::retry!(result, "failed to execute the statement");
	let group = row.map(|row| tg::group::Data {
		id: id.clone(),
		name: row.name,
		parent: row.parent,
		specifier,
	});

	Ok(ControlFlow::Break(group))
}

pub async fn resolve_parent_for_specifier_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	specifier: &tg::Specifier,
) -> tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>> {
	if specifier.components().next().is_none() {
		return Err(tg::error!("invalid specifier"));
	}
	let Some(parent) = specifier.parent() else {
		return Ok(ControlFlow::Break(None));
	};
	let parent =
		match crate::Session::try_get_id_for_specifier_with_transaction(transaction, &parent)
			.await?
		{
			ControlFlow::Break(parent) => parent,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		.ok_or_else(|| tg::error!("the parent does not exist"))?;
	if parent.kind() == tg::id::Kind::Tag {
		return Err(tg::error!("a tag cannot be a parent"));
	}

	Ok(ControlFlow::Break(Some(parent)))
}
