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
	pub(crate) async fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> tg::Result<Option<()>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => {
				self.remove_organization_member_local(organization, member)
					.await
			},
			tg::Location::Remote(remote) => {
				self.remove_organization_member_remote(organization, member, arg, remote)
					.await
			},
		}
	}

	async fn remove_organization_member_local(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
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
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					session
						.remove_organization_member_local_with_transaction(
							transaction,
							&organization,
							&member,
						)
						.await
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn remove_organization_member_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		let output = match self
			.remove_organization_member_with_transaction(
				transaction,
				organization,
				member,
				&mut batch,
			)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(output))
	}

	async fn remove_organization_member_remote(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		mut arg: tg::organization::members::remove::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client
			.remove_organization_member(organization, member, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to remove the organization member"),
			)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(output)
	}

	async fn remove_organization_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
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
		let organization_id: tg::Id = id.into();
		let member_id: tg::Id = member.clone().into();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from organization_members
				where organization = {p}1 and member = {p}2;
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![organization_id.to_string(), member_id.to_string()],
			)
			.await;
		let deleted = crate::database::retry!(result, "failed to execute the statement");
		if deleted == 0 {
			return Ok(ControlFlow::Break(None));
		}
		batch
			.items
			.push(tangram_index::batch::Item::DeleteOrganizationMember(
				tangram_index::organization::member::delete::Arg {
					member: member.clone(),
					organization: organization_id.clone().try_into()?,
				},
			));
		let subject = match member {
			tg::organization::Member::Group(id) => tg::authorization::Subject::Group(id.clone()),
			tg::organization::Member::User(id) => tg::authorization::Subject::User(id.clone()),
		};
		let arg = tg::grant::delete::Arg {
			permissions: tg::Either::Left(
				tg::authorization::Permission::Organization(
					tg::authorization::permission::organization::Permission::Write,
				)
				.into(),
			),
			resource: tg::Referent::with_node(tg::Selector::Id(organization_id)),
			subject: subject.into(),
		};
		match self
			.delete_grant_with_transaction(transaction, arg, batch)
			.await?
		{
			ControlFlow::Break(_) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(Some(())))
	}

	pub(crate) async fn remove_organization_member_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
		member: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let organization = organization.replace(':', "/").parse()?;
		let member = member.replace(':', "/").parse()?;
		let Some(()) = self
			.remove_organization_member(&organization, &member, arg)
			.await?
		else {
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
