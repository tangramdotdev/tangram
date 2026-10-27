use {
	futures::{FutureExt as _, future::BoxFuture},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

pub mod create;
pub mod delete;
pub mod get;
pub mod members;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn group_contains_group_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		group: &tg::Id,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<bool, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::Id,
		}
		let mut stack = vec![group.clone()];
		let mut visited = std::collections::BTreeSet::new();
		while let Some(group) = stack.pop() {
			if !visited.insert(group.clone()) {
				continue;
			}
			if &group == member {
				return Ok(ControlFlow::Break(true));
			}
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					select member
					from group_members
					where "group" = {p}1;
				"#
			);
			let result = transaction
				.query_all_into::<Row>(statement.into(), db::params![group.to_string()])
				.await;
			let rows =
				tangram_server_core::database::retry!(result, "failed to execute the statement");
			stack.extend(rows.into_iter().map(|row| row.member));
		}

		Ok(ControlFlow::Break(false))
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
			(http::Method::POST, ["groups"]) => self.create_group_request(session, request).await,
			(http::Method::GET, ["groups", group]) => {
				self.try_get_group_request(session, request, group).await
			},
			(http::Method::DELETE, ["groups", group]) => {
				self.try_delete_group_request(session, request, group).await
			},
			(http::Method::GET, ["groups", group, "members"]) => {
				self.list_group_members_request(session, request, group)
					.await
			},
			(http::Method::POST, ["groups", group, "members"]) => {
				self.add_group_member_request(session, request, group).await
			},
			(http::Method::DELETE, ["groups", group, "members", member]) => {
				self.remove_group_member_request(session, request, group, member)
					.await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::group::Component for Component {
	fn create_parent_groups_with_transaction<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		specifier: &'a tg::Specifier,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::Id>, tangram_server_core::database::Error>>>
	{
		self.create_parent_groups_with_transaction(session, transaction, specifier, batch)
			.boxed()
	}

	fn create_group<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::group::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::create::Output>> {
		self.create_group(session, arg).boxed()
	}

	fn try_get_group<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		group: &'a tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::group::get::Output>>> {
		self.try_get_group(session, group, arg).boxed()
	}

	fn try_delete_group<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		group: &'a tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_group(session, group, arg).boxed()
	}

	fn list_group_members<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		group: &'a tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::members::list::Output>> {
		self.list_group_members(session, group, arg).boxed()
	}

	fn add_group_member<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		group: &'a tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.add_group_member(session, group, arg).boxed()
	}

	fn remove_group_member<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		group: &'a tg::group::Selector,
		member: &'a tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.remove_group_member(session, group, member, arg)
			.boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}

	fn try_get_group_with_transaction<'a>(
		&'a self,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		id: &'a tg::group::Id,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::group::Data>, tangram_server_core::database::Error>>,
	> {
		Self::try_get_group_with_transaction(transaction, id).boxed()
	}

	fn resolve_parent_for_specifier_with_transaction<'a>(
		&'a self,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::Id>, tangram_server_core::database::Error>>>
	{
		Self::resolve_parent_for_specifier_with_transaction(transaction, specifier).boxed()
	}
}
