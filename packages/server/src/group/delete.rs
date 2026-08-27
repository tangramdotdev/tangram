use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_delete_group(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.try_delete_group_primary_region(group, arg).await
			},
			tg::Location::Local(_) => self.try_delete_group_local(group).await,
			tg::Location::Remote(remote) => self.try_delete_group_remote(group, arg, remote).await,
		}
	}

	async fn try_delete_group_local(&self, group: &tg::group::Selector) -> tg::Result<Option<()>> {
		let group = group.clone();
		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		let output = tangram_futures::retry(&options, || {
			let group = group.clone();
			let session = session.clone();
			async move {
				match session.try_delete_group_local_attempt(&group).await? {
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		self.server
			.spawn_publish_database_index_outbox_notification_task();
		self.checkout_index_barrier().await?;
		Ok(output)
	}

	async fn try_delete_group_local_attempt(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<ControlFlow<Option<()>>> {
		let selector = match group {
			tg::Selector::Id(id) => tg::Selector::Id(id.clone().into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier.clone()),
		};
		let Some((id, specifier)) = self.try_resolve_named_node(&selector).await? else {
			return Ok(ControlFlow::Break(None));
		};
		let Ok(group) = tg::group::Id::try_from(id.clone()) else {
			return Ok(ControlFlow::Break(None));
		};
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		);
		match self
			.authorize(tg::Selector::Id(group.clone()), permission)
			.await?
		{
			None => return Ok(ControlFlow::Break(None)),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}
		let ids_by_specifier = BTreeMap::from([(specifier, Some(id))]);
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let ids_by_specifier = ids_by_specifier.clone();
				let session = session.clone();
				async move {
					session
						.try_delete_group_local_with_transaction(
							transaction,
							&group,
							&ids_by_specifier,
						)
						.await
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn try_delete_group_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Id,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<ControlFlow<ControlFlow<Option<()>>, crate::database::Error>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
		match Self::verify_ids_for_specifiers_with_transaction(
			transaction,
			ids_by_specifier,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(true) => (),
			ControlFlow::Break(false) => {
				return Ok(ControlFlow::Break(ControlFlow::Continue(())));
			},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let mut batch = tangram_index::batch::Arg::default();
		let output = match self
			.delete_group_with_transaction(transaction, group, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match self
			.server
			.enqueue_database_index_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(ControlFlow::Break(output)))
	}

	async fn try_delete_group_primary_region(
		&self,
		group: &tg::group::Selector,
		mut arg: tg::group::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.try_delete_group(group, arg).await.map_err(|error| {
			tg::error!(!error, "failed to delete the group in the primary region")
		})?;

		Ok(output)
	}

	async fn try_delete_group_remote(
		&self,
		group: &tg::group::Selector,
		mut arg: tg::group::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.try_delete_group(group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to delete the group"),
		)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(output)
	}

	async fn delete_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Id,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		let data = match Self::try_get_group_with_transaction(transaction, group).await? {
			ControlFlow::Break(group) => group,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if data.is_none() {
			return Ok(ControlFlow::Break(None));
		}
		let id: tg::Id = group.clone().into();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select 1
				from groups
				where parent = {p}1
				union all
				select 1
				from tags
				where parent = {p}1
				limit 1;
			"
		);
		let result = transaction
			.query_optional(statement.into(), db::params![id.to_string()])
			.await;
		let child = crate::database::retry!(result, "failed to execute the statement");
		if child.is_some() {
			return Err(tg::error!("cannot delete a group with children"));
		}
		#[derive(db::row::Deserialize)]
		struct GroupMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			group: tg::group::Id,
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::group::Member,
		}
		let statement = format!(
			r#"
				select "group", member
				from group_members
				where "group" = {p}1 or member = {p}1;
			"#
		);
		let result = transaction
			.query_all_into::<GroupMemberRow>(statement.into(), db::params![id.to_string()])
			.await;
		let group_members = crate::database::retry!(result, "failed to execute the statement");
		for row in group_members {
			batch
				.items
				.push(tangram_index::batch::Item::DeleteGroupMember(
					tangram_index::group::member::delete::Arg {
						group: row.group,
						member: row.member,
					},
				));
		}
		#[derive(db::row::Deserialize)]
		struct OrganizationMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::organization::Member,
			#[tangram_database(as = "db::value::FromStr")]
			organization: tg::organization::Id,
		}
		let statement = format!(
			"
				select organization, member
				from organization_members
				where member = {p}1;
			"
		);
		let result = transaction
			.query_all_into::<OrganizationMemberRow>(statement.into(), db::params![id.to_string()])
			.await;
		let organization_members =
			crate::database::retry!(result, "failed to execute the statement");
		for row in organization_members {
			batch
				.items
				.push(tangram_index::batch::Item::DeleteOrganizationMember(
					tangram_index::organization::member::delete::Arg {
						member: row.member,
						organization: row.organization,
					},
				));
		}
		match self
			.delete_node_grants_with_transaction(transaction, &id, batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		batch.items.push(tangram_index::batch::Item::DeleteGroup(
			id.clone().try_into()?,
		));
		for statement in [
			format!("delete from group_members where \"group\" = {p}1 or member = {p}1;"),
			format!("delete from organization_members where member = {p}1;"),
			format!("delete from groups where id = {p}1;"),
			format!("delete from specifiers where id = {p}1;"),
		] {
			let result = transaction
				.execute(statement.into(), db::params![id.to_string()])
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		}

		Ok(ControlFlow::Break(Some(())))
	}

	pub(crate) async fn try_delete_group_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let group = group.replace(':', "/").parse()?;
		let Some(()) = self.try_delete_group(&group, arg).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
