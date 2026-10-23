use {
	crate::{Session, database::Transaction},
	futures::future::BoxFuture,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::body::Boxed as BoxBody,
};

/// The tag component.
pub trait Component: crate::component::Lifecycle {
	fn delete_tags<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'a, tg::Result<tg::tag::delete::Output>>;

	fn put_tag<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_tag_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::tag::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_tag<'a>(
		&'a self,
		session: &'a Session,
		tag: &'a tg::tag::Selector,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn pull_tag<'a>(
		&'a self,
		session: &'a Session,
		pattern: tg::specifier::Pattern,
		location: Option<tg::location::Arg>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_tag_local<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::tag::Id,
		tokens: tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn try_get_tag_remote<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::tag::Id,
		arg: tg::tag::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;
}

pub fn parse_tag_target(target: &str) -> tg::Result<tg::tag::data::Target> {
	target
		.parse::<tg::Either<tg::object::Id, tg::process::Id>>()
		.map(Into::into)
		.map_err(|error| tg::error!(!error, "failed to parse the tag target"))
}

pub async fn try_get_tag_data_with_transaction(
	transaction: &Transaction<'_>,
	id: &tg::tag::Id,
) -> tg::Result<ControlFlow<Option<tg::tag::Data>, crate::database::Error>> {
	#[derive(db::row::Deserialize)]
	struct Row {
		name: String,
		#[tangram_database(as = "Option<db::value::FromStr>")]
		parent: Option<tg::Id>,
		target: String,
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
			select target, name, parent
			from tags
			where id = {p}1;
		"
	);
	let result = transaction
		.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
		.await;
	let row = crate::database::retry!(result, "failed to execute the statement");
	let Some(row) = row else {
		return Ok(ControlFlow::Break(None));
	};
	let target = parse_tag_target(&row.target)?;
	let data = tg::tag::Data {
		id: id.clone(),
		name: row.name,
		parent: row.parent,
		specifier,
		target,
	};

	Ok(ControlFlow::Break(Some(data)))
}

pub async fn get_tag_data_with_transaction(
	transaction: &Transaction<'_>,
	id: &tg::tag::Id,
) -> tg::Result<ControlFlow<tg::tag::Data, crate::database::Error>> {
	let data = match try_get_tag_data_with_transaction(transaction, id).await? {
		ControlFlow::Break(data) => data,
		ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
	}
	.ok_or_else(|| tg::error!("failed to find the tag"))?;

	Ok(ControlFlow::Break(data))
}

#[must_use]
pub fn version() -> String {
	tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}
