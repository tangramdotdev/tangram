use {
	crate::Session,
	futures::future::BoxFuture,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::body::Boxed as BoxBody,
};

/// The grant component.
pub trait Component: crate::component::Lifecycle {
	fn create_grant<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::grant::create::Output>>;

	fn delete_grant<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_grants<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::list::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::list::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn create_grant_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		arg: tg::grant::create::Arg,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(tg::grant::Data, bool), crate::database::Error>>>;

	fn delete_grant_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		arg: tg::grant::delete::Arg,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<()>, crate::database::Error>>>;

	fn delete_node_grants_batch_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		ids: &'a [tg::Id],
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;

	fn delete_node_grants_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::Id,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;
}

pub fn admin_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Admin,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Admin,
		)),
		_ => Err(tg::error!("invalid resource")),
	}
}

pub async fn list_resource_grants_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	resource: &tg::Id,
	after: Option<&(String, String)>,
	limit: Option<u64>,
) -> tg::Result<ControlFlow<Vec<tg::grant::Data>, crate::database::Error>> {
	#[derive(db::row::Deserialize)]
	struct Row {
		created_at: i64,
		#[tangram_database(as = "db::value::FromStr")]
		creator: tg::Principal,
		#[tangram_database(as = "db::value::FromStr")]
		permissions: tg::authorization::permission::Set,
		#[tangram_database(as = "db::value::FromStr")]
		subject: tg::authorization::Subject,
	}
	let p = transaction.p();
	let (after, creator) = after
		.map(|(after, creator)| (after.as_str(), creator.as_str()))
		.unwrap_or_default();
	let limit = limit.map_or(i64::MAX, |limit| i64::try_from(limit).unwrap());
	let statement = formatdoc!(
		"
			select created_at, creator, permissions, subject
			from grants
			where resource = {p}1
			and (subject, creator) > ({p}2, {p}3)
			order by subject, creator
			limit {p}4;
		"
	);
	let result = transaction
		.query_all_into::<Row>(
			statement.into(),
			db::params![resource.to_string(), after, creator, limit],
		)
		.await;
	let rows = crate::database::retry!(result, "failed to execute the statement");
	let grants = rows
		.into_iter()
		.map(|row| tg::grant::Data {
			created_at: row.created_at,
			creator: Some(row.creator),
			permissions: row.permissions,
			resource: resource.clone(),
			subject: row.subject,
		})
		.collect();

	Ok(ControlFlow::Break(grants))
}

pub fn read_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Read,
		)),
		tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Node,
		)),
		tg::id::Kind::Sync => Ok(tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Read,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Read,
		)),
		_ => Err(tg::error!("invalid resource")),
	}
}

pub async fn resolve_subject_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	subject: &tg::authorization::subject::Selector,
) -> tg::Result<ControlFlow<Option<tg::authorization::Subject>, crate::database::Error>> {
	let subject = match subject {
		tg::authorization::subject::Selector::Subject(subject) => match subject {
			tg::authorization::Subject::Group(id) => {
				let id = id.clone();
				let specifier = match crate::Session::try_get_specifier_for_id_with_transaction(
					transaction,
					&id.clone().into(),
				)
				.await?
				{
					ControlFlow::Break(specifier) => specifier,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				if specifier.is_none() {
					return Ok(ControlFlow::Break(None));
				}
				tg::authorization::Subject::Group(id)
			},
			tg::authorization::Subject::Organization(id) => {
				let id = id.clone();
				let specifier = match crate::Session::try_get_specifier_for_id_with_transaction(
					transaction,
					&id.clone().into(),
				)
				.await?
				{
					ControlFlow::Break(specifier) => specifier,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				if specifier.is_none() {
					return Ok(ControlFlow::Break(None));
				}
				tg::authorization::Subject::Organization(id)
			},
			tg::authorization::Subject::Process(id) => {
				tg::authorization::Subject::Process(id.clone())
			},
			tg::authorization::Subject::Public => tg::authorization::Subject::Public,
			tg::authorization::Subject::Root => tg::authorization::Subject::Root,
			tg::authorization::Subject::Runner(id) => {
				tg::authorization::Subject::Runner(id.clone())
			},
			tg::authorization::Subject::Sandbox(id) => {
				tg::authorization::Subject::Sandbox(id.clone())
			},
			tg::authorization::Subject::Sync(id) => tg::authorization::Subject::Sync(id.clone()),
			tg::authorization::Subject::Tag(id) => tg::authorization::Subject::Tag(id.clone()),
			tg::authorization::Subject::User(id) => {
				let id = id.clone();
				let specifier = match crate::Session::try_get_specifier_for_id_with_transaction(
					transaction,
					&id.clone().into(),
				)
				.await?
				{
					ControlFlow::Break(specifier) => specifier,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				if specifier.is_none() {
					return Ok(ControlFlow::Break(None));
				}
				tg::authorization::Subject::User(id)
			},
		},
		tg::authorization::subject::Selector::Specifier(specifier) => {
			let id = match crate::Session::try_get_id_for_specifier_with_transaction(
				transaction,
				specifier,
			)
			.await?
			{
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let Some(id) = id else {
				return Ok(ControlFlow::Break(None));
			};
			match id.kind() {
				tg::id::Kind::Group => tg::authorization::Subject::Group(id.try_into()?),
				tg::id::Kind::Organization => {
					tg::authorization::Subject::Organization(id.try_into()?)
				},
				tg::id::Kind::User => tg::authorization::Subject::User(id.try_into()?),
				_ => return Ok(ControlFlow::Break(None)),
			}
		},
	};

	Ok(ControlFlow::Break(Some(subject)))
}

pub fn write_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Write,
		)),
		tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Parent,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Write,
		)),
		_ => Err(tg::error!("invalid resource")),
	}
}
