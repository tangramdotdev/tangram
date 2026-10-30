use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn add_organization_member(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> tg::Result<()> {
		let location = session
			.server
			.account_location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		if location.is_local() && !session.server.config.accounts {
			return Err(tg::error!("local account management is disabled"));
		}
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.add_organization_member_region(session, organization, arg, region)
					.await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.add_organization_member_primary_region(session, organization, arg)
					.await
			},
			tg::Location::Local(_) => {
				self.add_organization_member_local(session, organization, &arg.member, &arg.tokens)
					.await
			},
			tg::Location::Remote(remote) => {
				self.add_organization_member_remote(session, organization, arg, remote)
					.await
			},
		}
	}

	async fn add_organization_member_local(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<()> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		session
			.authorize(
				tg::Referent::with_node_and_tokens(organization.clone(), tokens.clone()),
				permission,
			)
			.await?
			.into_result()?;
		let component = self.clone();
		let session = session.clone();
		session
			.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let member = member.clone();
				let component = component.clone();
				let session = session.clone();
				async move {
					component
						.add_organization_member_local_with_transaction(
							&session,
							transaction,
							&organization,
							&member,
						)
						.await
				}
				.boxed()
			})
			.await?;
		session
			.server
			.components
			.indexer
			.spawn_publish_database_index_queue_notification_task(&session.server);
		Ok(())
	}

	async fn add_organization_member_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let mut batch = tangram_index::write::Arg::default();
		match self
			.add_organization_member_with_transaction(
				session,
				transaction,
				organization,
				member,
				&mut batch,
			)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		match session
			.server
			.components
			.indexer
			.enqueue_database_index_queue_with_transaction(&session.server, transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	async fn add_organization_member_primary_region(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::add::Arg,
	) -> tg::Result<()> {
		let client = session
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

	async fn add_organization_member_region(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::add::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		client
			.add_organization_member(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to add the organization member"),
			)?;

		Ok(())
	}

	async fn add_organization_member_remote(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::add::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		arg.tokens = arg.tokens.for_location(&location);
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		client
			.add_organization_member(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to add the organization member"),
			)?;
		session
			.server
			.components
			.remote
			.invalidate_remote_cache(session, &remote.name)
			.await;

		Ok(())
	}

	async fn add_organization_member_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		batch: &mut tangram_index::write::Arg,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let id = match organization {
			tg::Selector::Id(id) => Some(id.clone()),
			tg::Selector::Specifier(specifier) => {
				let id =
					match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
						transaction,
						specifier,
					)
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
			match tangram_server_node_common::specifier::try_get_specifier_for_id_with_transaction(
				transaction,
				&member_id,
			)
			.await?
			{
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
		let inserted =
			tangram_server_core::database::retry!(result, "failed to execute the statement");
		if inserted == 0 {
			return Err(tg::error!("the member is already in the organization"));
		}
		batch.items.push(tangram_index::Item::PutOrganizationMember(
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
		match session
			.server
			.components
			.grant
			.create_grant_with_transaction(session, transaction, arg, batch)
			.await?
		{
			ControlFlow::Break(_) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn add_organization_member_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg: tg::organization::members::add::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let organization = organization
			.replace(':', "/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the organization"))?;
		self.add_organization_member(session, &organization, arg)
			.await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
