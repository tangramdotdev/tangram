use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.try_delete_organization_local(organization).await,
			tg::Location::Remote(remote) => {
				self.try_delete_organization_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn try_delete_organization_local(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<()>> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		match self.authorize(organization.clone(), permission).await? {
			None => return Ok(None),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}
		let session = self.clone();
		let output = self
			.server
			.run_database_outbox_transaction(|transaction, database_outbox_partition| {
				let organization = organization.clone();
				let session = session.clone();
				async move {
					session
						.try_delete_organization_local_with_transaction(
							transaction,
							&organization,
							database_outbox_partition,
						)
						.await
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn try_delete_organization_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		database_outbox_partition: u64,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		let output = match self
			.delete_organization_with_transaction(transaction, organization, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match self
			.server
			.enqueue_database_outbox_with_transaction(
				transaction,
				database_outbox_partition,
				&batch,
			)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(output))
	}

	async fn try_delete_organization_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client
			.try_delete_organization(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to delete the organization"),
			)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(output)
	}

	async fn delete_organization_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		let id = match organization {
			tg::Selector::Id(id) => Some(id.clone()),
			tg::Selector::Specifier(specifier) => {
				let id =
					match Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
						.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				id.and_then(|id| id.try_into().ok())
			},
		};
		let Some(id) = id else {
			return Ok(ControlFlow::Break(None));
		};
		let organization =
			match Self::try_get_organization_with_transaction(transaction, &id).await? {
				ControlFlow::Break(organization) => organization,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if organization.is_none() {
			return Ok(ControlFlow::Break(None));
		}
		let id: tg::Id = id.into();
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
			return Err(tg::error!("cannot delete an organization with children"));
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
				where organization = {p}1;
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
		batch
			.items
			.push(tangram_index::batch::Item::DeleteOrganization(
				id.clone().try_into()?,
			));
		for statement in [
			format!("delete from organization_members where organization = {p}1;"),
			format!("delete from organizations where id = {p}1;"),
			format!("delete from specifiers where id = {p}1;"),
		] {
			let result = transaction
				.execute(statement.into(), db::params![id.to_string()])
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		}

		Ok(ControlFlow::Break(Some(())))
	}

	pub(crate) async fn try_delete_organization_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let organization = organization.replace(':', "/").parse()?;
		let Some(()) = self.try_delete_organization(&organization, arg).await? else {
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
