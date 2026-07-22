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
		let permission = tg::grant::Permission::Organization(
			tg::grant::permission::organization::Permission::Admin,
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
					let mut batch = tangram_index::batch::Arg::default();
					let output = session
						.remove_organization_member_with_transaction(
							transaction,
							&organization,
							&member,
							&mut batch,
						)
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
		client
			.remove_organization_member(organization, member, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to remove the organization member"),
			)
	}

	async fn remove_organization_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<()>> {
		let Some(organization) =
			Self::try_get_node_by_selector_with_transaction(transaction, organization).await?
		else {
			return Ok(None);
		};
		let member_id: tg::Id = member.clone().into();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from organization_members
				where organization = {p}1 and member = {p}2;
			"
		);
		let deleted = transaction
			.execute(
				statement.into(),
				db::params![organization.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if deleted == 0 {
			return Ok(None);
		}
		batch
			.delete_organization_members
			.push(tangram_index::organization::member::delete::Arg {
				member: member.clone(),
				organization: organization.id.clone().try_into()?,
			});
		let principal = match member {
			tg::organization::Member::Group(id) => tg::grant::Principal::Group(id.clone()),
			tg::organization::Member::User(id) => tg::grant::Principal::User(id.clone()),
		};
		let arg = tg::grant::delete::Arg {
			principal: principal.into(),
			permissions: tg::Either::Left(
				tg::grant::Permission::Organization(
					tg::grant::permission::organization::Permission::Write,
				)
				.into(),
			),
			resource: tg::Referent::with_item(tg::grant::Resource::Id(organization.id.clone())),
		};
		self.delete_grant_with_transaction(transaction, arg, batch)
			.await?;
		Ok(Some(()))
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
