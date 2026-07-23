use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
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
			tg::Location::Local(_) => self.try_delete_group_local(group).await,
			tg::Location::Remote(remote) => self.try_delete_group_remote(group, arg, remote).await,
		}
	}

	async fn try_delete_group_local(&self, group: &tg::group::Selector) -> tg::Result<Option<()>> {
		let permission =
			tg::grant::Permission::Group(tg::grant::permission::group::Permission::Admin);
		match self.authorize(group.clone(), permission).await? {
			None => return Ok(None),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let output = session
						.delete_group_with_transaction(transaction, &group, &mut batch)
						.await?;
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await?;
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
		client.try_delete_group(group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to delete the group"),
		)
	}

	async fn delete_group_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<()>> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, group).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Group {
			return Ok(None);
		}
		if Self::node_has_children_with_transaction(transaction, &node.id).await? {
			return Err(tg::error!("cannot delete a group with children"));
		}
		let p = transaction.p();
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
		let group_members = transaction
			.query_all_into::<GroupMemberRow>(statement.into(), db::params![node.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
		let organization_members = transaction
			.query_all_into::<OrganizationMemberRow>(
				statement.into(),
				db::params![node.id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
		self.delete_node_grants_with_transaction(transaction, &node.id, batch)
			.await?;
		batch.items.push(tangram_index::batch::Item::DeleteGroup(
			node.id.clone().try_into()?,
		));
		for statement in [
			format!("delete from group_members where \"group\" = {p}1 or member = {p}1;"),
			format!("delete from organization_members where member = {p}1;"),
			format!("delete from groups where id = {p}1;"),
			format!("delete from nodes where id = {p}1;"),
		] {
			transaction
				.execute(statement.into(), db::params![node.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(Some(()))
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
