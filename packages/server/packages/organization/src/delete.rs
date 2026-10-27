use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn try_delete_organization(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> tg::Result<Option<()>> {
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
				self.try_delete_organization_region(session, organization, arg, region)
					.await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.try_delete_organization_primary_region(session, organization, arg)
					.await
			},
			tg::Location::Local(_) => {
				self.try_delete_organization_local(session, organization, &arg.tokens)
					.await
			},
			tg::Location::Remote(remote) => {
				self.try_delete_organization_remote(session, organization, arg, remote)
					.await
			},
		}
	}

	async fn try_delete_organization_local(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<()>> {
		let organization = organization.clone();
		let tokens = tokens.clone();
		let options = session
			.server
			.config
			.verification
			.write_retry
			.clone()
			.into();
		let component = self.clone();
		let session = session.clone();
		let output = tangram_futures::retry(&options, || {
			let organization = organization.clone();
			let component = component.clone();
			let session = session.clone();
			let tokens = tokens.clone();
			async move {
				match component
					.try_delete_organization_local_attempt(&session, &organization, &tokens)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		session
			.server
			.components
			.indexer
			.spawn_publish_database_index_queue_notification_task(&session.server);
		session
			.server
			.components
			.checkout
			.checkout_await_indexing(&session)
			.await?;
		Ok(output)
	}

	async fn try_delete_organization_local_attempt(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<ControlFlow<Option<()>>> {
		let selector = match organization {
			tg::Selector::Id(id) => tg::Selector::Id(id.clone().into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier.clone()),
		};
		let Some((id, specifier)) = session
			.server
			.components
			.node
			.try_resolve_named_node(session, &selector)
			.await?
		else {
			return Ok(ControlFlow::Break(None));
		};
		let Ok(organization) = tg::organization::Id::try_from(id.clone()) else {
			return Ok(ControlFlow::Break(None));
		};
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		session
			.authorize(
				tg::Referent::with_node_and_tokens(
					tg::Selector::Id(organization.clone()),
					tokens.clone(),
				),
				permission,
			)
			.await?
			.into_result()?;
		let ids_by_specifier = BTreeMap::from([(specifier, Some(id))]);
		let component = self.clone();
		let session = session.clone();
		let output = session
			.server
			.database
			.run(|transaction| {
				let ids_by_specifier = ids_by_specifier.clone();
				let organization = organization.clone();
				let component = component.clone();
				let session = session.clone();
				async move {
					component
						.try_delete_organization_local_with_transaction(
							&session,
							transaction,
							&organization,
							&ids_by_specifier,
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
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		organization: &tg::organization::Id,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<ControlFlow<ControlFlow<Option<()>>, tangram_server_core::database::Error>> {
		let batch_size = session.server.config.sync.get.database.batch_size;
		match tangram_server_node_common::specifier::verify_ids_for_specifiers_with_transaction(
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
			.delete_organization_with_transaction(session, transaction, organization, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
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

		Ok(ControlFlow::Break(ControlFlow::Break(output)))
	}

	async fn try_delete_organization_primary_region(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client
			.try_delete_organization(organization, arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to delete the organization in the primary region"
				)
			})?;

		Ok(output)
	}

	async fn try_delete_organization_region(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::delete::Arg,
		region: String,
	) -> tg::Result<Option<()>> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		let output = client
			.try_delete_organization(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to delete the organization"),
			)?;

		Ok(output)
	}

	async fn try_delete_organization_remote(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
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
		let output = client
			.try_delete_organization(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to delete the organization"),
			)?;
		session
			.server
			.components
			.remote
			.invalidate_remote_cache(session, &remote.name)
			.await;

		Ok(output)
	}

	async fn delete_organization_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		organization: &tg::organization::Id,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<()>, tangram_server_core::database::Error>> {
		let data =
			match Self::try_get_organization_with_transaction(transaction, organization).await? {
				ControlFlow::Break(organization) => organization,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if data.is_none() {
			return Ok(ControlFlow::Break(None));
		}
		let id: tg::Id = organization.clone().into();
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
		let child =
			tangram_server_core::database::retry!(result, "failed to execute the statement");
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
			tangram_server_core::database::retry!(result, "failed to execute the statement");
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
		match session
			.server
			.components
			.grant
			.delete_node_grants_with_transaction(session, transaction, &id, batch)
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
			tangram_server_core::database::retry!(result, "failed to execute the statement");
		}

		Ok(ControlFlow::Break(Some(())))
	}

	pub(crate) async fn try_delete_organization_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let organization = organization
			.replace(':', "/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the organization"))?;
		let Some(()) = self
			.try_delete_organization(session, &organization, arg)
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
