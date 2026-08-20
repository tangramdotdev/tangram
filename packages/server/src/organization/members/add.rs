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
	pub(crate) async fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> tg::Result<()> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.add_organization_member_primary_region(organization, arg)
					.await
			},
			tg::Location::Local(_) => {
				self.add_organization_member_local(organization, &arg.member)
					.await
			},
			tg::Location::Remote(remote) => {
				self.add_organization_member_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn add_organization_member_local(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<()> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		match self.authorize(organization.clone(), permission).await? {
			None => return Err(tg::error!("failed to find the organization")),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					session
						.add_organization_member_local_with_transaction(
							transaction,
							&organization,
							&member,
						)
						.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		Ok(())
	}

	async fn add_organization_member_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		match self
			.add_organization_member_with_transaction(transaction, organization, member, &mut batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	async fn add_organization_member_primary_region(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::add::Arg,
	) -> tg::Result<()> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.add_organization_member(organization, arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to add the organization member in the primary region"
				)
			})?;

		Ok(())
	}

	async fn add_organization_member_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::add::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.add_organization_member(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to add the organization member"),
			)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(())
	}

	async fn add_organization_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
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
		}
		.ok_or_else(|| tg::error!("failed to find the organization"))?;
		let organization =
			match Self::try_get_organization_with_transaction(transaction, &id).await? {
				ControlFlow::Break(organization) => organization,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if organization.is_none() {
			return Err(tg::error!("failed to find the organization"));
		}
		let organization_id: tg::Id = id.into();
		let member_id: tg::Id = member.clone().into();
		let specifier =
			match Self::try_get_specifier_for_id_with_transaction(transaction, &member_id).await? {
				ControlFlow::Break(specifier) => specifier,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if specifier.is_none() {
			return Err(tg::error!("failed to find the member"));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into organization_members (organization, member)
				values ({p}1, {p}2)
				on conflict (organization, member) do nothing;
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![organization_id.to_string(), member_id.to_string()],
			)
			.await;
		let inserted = crate::database::retry!(result, "failed to execute the statement");
		if inserted == 0 {
			return Err(tg::error!("the member is already in the organization"));
		}
		batch
			.items
			.push(tangram_index::batch::Item::PutOrganizationMember(
				tangram_index::organization::member::put::Arg {
					member: member.clone(),
					organization: organization_id.clone().try_into()?,
				},
			));
		let subject = match member {
			tg::organization::Member::Group(id) => tg::authorization::Subject::Group(id.clone()),
			tg::organization::Member::User(id) => tg::authorization::Subject::User(id.clone()),
		};
		let arg = tg::grant::create::Arg {
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
			.create_grant_with_transaction(transaction, arg, batch)
			.await?
		{
			ControlFlow::Break(_) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn add_organization_member_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg: tg::organization::members::add::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let organization = organization.replace(':', "/").parse()?;
		self.add_organization_member(&organization, arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
