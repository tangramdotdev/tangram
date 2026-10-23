use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn create_organization(
		&self,
		session: &Session,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
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
				self.create_organization_region(session, arg, region).await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.create_organization_primary_region(session, arg).await
			},
			tg::Location::Local(_) => self.create_organization_local(session, arg).await,
			tg::Location::Remote(remote) => {
				self.create_organization_remote(session, arg, remote).await
			},
		}
	}

	async fn create_organization_local(
		&self,
		session: &Session,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
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
			let arg = arg.clone();
			let component = component.clone();
			let session = session.clone();
			async move {
				match component
					.create_organization_local_attempt(&session, arg)
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
			.spawn_publish_database_index_queue_notification_task();
		Ok(output)
	}

	async fn create_organization_local_attempt(
		&self,
		session: &Session,
		arg: tg::organization::create::Arg,
	) -> tg::Result<ControlFlow<tg::organization::create::Output>> {
		let ids_by_specifier = session
			.try_get_ids_and_ancestors_for_specifiers(std::slice::from_ref(&arg.specifier))
			.await?;
		let component = self.clone();
		let session = session.clone();
		let output = session
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let ids_by_specifier = ids_by_specifier.clone();
				let component = component.clone();
				let session = session.clone();
				async move {
					component
						.create_organization_local_with_transaction(
							&session,
							transaction,
							arg,
							&ids_by_specifier,
						)
						.await
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn create_organization_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::organization::create::Arg,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<
		ControlFlow<
			ControlFlow<tg::organization::create::Output>,
			tangram_server_core::database::Error,
		>,
	> {
		let batch_size = session.server.config.sync.get.database.batch_size;
		match tangram_server_core::Session::verify_ids_for_specifiers_with_transaction(
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
		let organization = match self
			.create_organization_with_transaction(session, transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(organization) => organization,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match session
			.server
			.enqueue_database_index_queue_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let output = tg::organization::create::Output {
			data: organization.data,
			location: organization.location,
			tokens: organization.tokens,
		};

		Ok(ControlFlow::Break(ControlFlow::Break(output)))
	}

	async fn create_organization_primary_region(
		&self,
		session: &Session,
		mut arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.create_organization(arg).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to create the organization in the primary region"
			)
		})?;

		Ok(output)
	}

	async fn create_organization_region(
		&self,
		session: &Session,
		mut arg: tg::organization::create::Arg,
		region: String,
	) -> tg::Result<tg::organization::create::Output> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		let mut output = client.create_organization(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to create the organization"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region),
		});
		output.location = Some(location);

		Ok(output)
	}

	async fn create_organization_remote(
		&self,
		session: &Session,
		mut arg: tg::organization::create::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::organization::create::Output> {
		let client = session.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		let mut output = client.create_organization(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to create the organization"),
		)?;
		session.invalidate_remote_cache(&remote.name).await;
		let location = tg::Location::Remote(remote);
		session.update_tokens_and_location(
			&mut output.tokens,
			Some(&mut output.location),
			&location,
			trusted,
		)?;

		Ok(output)
	}

	async fn create_organization_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::organization::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<
		ControlFlow<tg::organization::create::Output, tangram_server_core::database::Error>,
	> {
		if arg.specifier.components().count() != 1 {
			return Err(tg::error!("invalid organization specifier"));
		}
		let id = match tangram_server_core::Session::try_get_id_for_specifier_with_transaction(
			transaction,
			&arg.specifier,
		)
		.await?
		{
			ControlFlow::Break(id) => id,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if id.is_some() {
			return Err(tg::error!("specifier is already in use"));
		}
		let id = tg::organization::Id::new();
		match tangram_server_core::Session::insert_specifier_with_transaction(
			transaction,
			&id.clone().into(),
			&arg.specifier,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let name = arg.specifier.name().to_owned();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into organizations (id, name)
				values ({p}1, {p}2);
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![id.to_string(), name.clone()])
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");
		batch
			.items
			.push(tangram_index::batch::Item::PutOrganization(
				tangram_index::organization::put::Arg {
					billing_ready: Some(false),
					id: id.clone(),
					specifier: arg.specifier.clone(),
				},
			));
		if !matches!(
			session.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			let principal = &session.context.principal;
			let arg = tg::grant::create::Arg {
				subject: principal.try_to_subject()?.into(),
				permissions: tg::Either::Left(
					tg::authorization::Permission::Organization(
						tg::authorization::permission::organization::Permission::Admin,
					)
					.into(),
				),
				resource: tg::Referent::with_node(tg::Selector::Id(id.clone().into())),
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
		}
		let tokens = tg::authorization::Tokens::with_authorization(
			session.create_read_token(&id.clone().into())?,
		);
		let data = tg::organization::Data {
			id,
			name,
			specifier: arg.specifier,
		};
		let organization = tg::organization::create::Output {
			data,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
		};

		Ok(ControlFlow::Break(organization))
	}

	pub(crate) async fn create_organization_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_organization(session, arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}
}
