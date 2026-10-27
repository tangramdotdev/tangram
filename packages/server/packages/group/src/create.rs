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
	pub(crate) async fn create_group(
		&self,
		session: &Session,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
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
				self.create_group_region(session, arg, region).await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.create_group_primary_region(session, arg).await
			},
			tg::Location::Local(_) => self.create_group_local(session, arg).await,
			tg::Location::Remote(remote) => self.create_group_remote(session, arg, remote).await,
		}
	}

	async fn create_group_local(
		&self,
		session: &Session,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		session
			.server
			.components
			.node
			.pull_ancestors(session, &arg.specifier, arg.ancestors.pull)
			.await?;
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
				match component.create_group_local_attempt(&session, arg).await? {
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
		Ok(output)
	}

	async fn create_group_local_attempt(
		&self,
		session: &Session,
		arg: tg::group::create::Arg,
	) -> tg::Result<ControlFlow<tg::group::create::Output>> {
		let ids_by_specifier = session
			.server
			.components
			.node
			.try_get_ids_and_ancestors_for_specifiers(session, std::slice::from_ref(&arg.specifier))
			.await?;
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		);
		let authorized = session
			.authorize(
				tg::Selector::<tg::Id>::Specifier(arg.specifier.clone()),
				permission,
			)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission)
			&& ids_by_specifier.values().any(Option::is_some)
		{
			return Err(tg::error!("unauthorized"));
		}
		tangram_server_core::checkpoint!(session.server, "group.create.authorized", specifier = %arg.specifier)
			.await;
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
						.create_group_local_with_transaction(
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

	async fn create_group_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::group::create::Arg,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<
		ControlFlow<ControlFlow<tg::group::create::Output>, tangram_server_core::database::Error>,
	> {
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
		let group = match self
			.create_group_with_transaction(session, transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(group) => group,
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
		let output = tg::group::create::Output {
			data: group.data,
			location: group.location,
			tokens: group.tokens,
		};

		Ok(ControlFlow::Break(ControlFlow::Break(output)))
	}

	async fn create_group_primary_region(
		&self,
		session: &Session,
		mut arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.create_group(arg).await.map_err(|error| {
			tg::error!(!error, "failed to create the group in the primary region")
		})?;

		Ok(output)
	}

	async fn create_group_region(
		&self,
		session: &Session,
		mut arg: tg::group::create::Arg,
		region: String,
	) -> tg::Result<tg::group::create::Output> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		let mut output = client
			.create_group(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to create the group"))?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region),
		});
		output.location = Some(location);

		Ok(output)
	}

	async fn create_group_remote(
		&self,
		session: &Session,
		mut arg: tg::group::create::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::group::create::Output> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		let mut output = client.create_group(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to create the group"),
		)?;
		session
			.server
			.components
			.remote
			.invalidate_remote_cache(session, &remote.name)
			.await;
		let location = tg::Location::Remote(remote);
		session
			.server
			.components
			.authorization
			.update_tokens_and_location(
				session,
				&mut output.tokens,
				Some(&mut output.location),
				&location,
				trusted,
			)?;

		Ok(output)
	}

	async fn create_group_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::group::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::group::create::Output, tangram_server_core::database::Error>> {
		let id =
			match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
				transaction,
				&arg.specifier,
			)
			.await?
			{
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if let Some(id) = id {
			if arg.ancestors.create && id.kind() == tg::id::Kind::Group {
				let id = id.try_into()?;
				let group = match Self::try_get_group_with_transaction(transaction, &id).await? {
					ControlFlow::Break(group) => group,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let data = group.ok_or_else(|| tg::error!("failed to find the group"))?;
				let mut tokens = tg::authorization::Tokens::default();
				if let Some(token) = session
					.server
					.components
					.authorization
					.create_read_token(session, &id.clone().into())?
				{
					tokens.insert_local_authorization(token);
				}
				let group = tg::group::create::Output {
					data,
					location: Some(tg::Location::Local(tg::location::Local::default())),
					tokens,
				};

				return Ok(ControlFlow::Break(group));
			}
			return Err(tg::error!("specifier is already in use"));
		}
		let parent = if arg.ancestors.create {
			match self
				.create_parent_groups_with_transaction(session, transaction, &arg.specifier, batch)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		} else {
			match Self::resolve_parent_for_specifier_with_transaction(transaction, &arg.specifier)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		};
		let group = match self
			.insert_group_with_transaction(
				session,
				transaction,
				&arg.specifier,
				parent.as_ref(),
				batch,
			)
			.await?
		{
			ControlFlow::Break(group) => group,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let mut tokens = tg::authorization::Tokens::default();
		if let Some(token) = session
			.server
			.components
			.authorization
			.create_read_token(session, &group.id.clone().into())?
		{
			tokens.insert_local_authorization(token);
		}
		let group = tg::group::create::Output {
			data: group,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
		};

		Ok(ControlFlow::Break(group))
	}

	pub async fn create_parent_groups_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		specifier: &tg::Specifier,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<tg::Id>, tangram_server_core::database::Error>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let mut parent = None;
		for specifier in specifier.ancestors() {
			let id = match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
				transaction,
				&specifier,
			)
			.await?
			{
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let id = if let Some(id) = id {
				id
			} else {
				let group = match self
					.insert_group_with_transaction(
						session,
						transaction,
						&specifier,
						parent.as_ref(),
						batch,
					)
					.await?
				{
					ControlFlow::Break(group) => group,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				group.id.into()
			};
			if id.kind() == tg::id::Kind::Tag {
				return Err(tg::error!("a tag cannot be a parent"));
			}
			parent = Some(id);
		}

		Ok(ControlFlow::Break(parent))
	}

	async fn insert_group_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		specifier: &tg::Specifier,
		parent: Option<&tg::Id>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<tg::group::Data, tangram_server_core::database::Error>> {
		let id = tg::group::Id::new();
		match tangram_server_node_common::specifier::insert_specifier_with_transaction(
			transaction,
			&id.clone().into(),
			specifier,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let name = specifier.name().to_owned();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into groups (id, name, parent)
				values ({p}1, {p}2, {p}3);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![
					id.to_string(),
					name.clone(),
					parent.map(ToString::to_string)
				],
			)
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");
		batch.items.push(tangram_index::batch::Item::PutGroup(
			tangram_index::group::put::Arg {
				id: id.clone(),
				parent: parent.cloned(),
				specifier: specifier.clone(),
			},
		));
		if !matches!(
			session.context.principal,
			tg::Principal::Anonymous | tg::Principal::Root
		) {
			let subject = session.context.principal.try_to_subject()?;
			let arg = tg::grant::create::Arg {
				permissions: tg::Either::Left(
					tg::authorization::Permission::Group(
						tg::authorization::permission::group::Permission::Admin,
					)
					.into(),
				),
				resource: tg::Referent::with_node(tg::Selector::Id(id.clone().into())),
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
		}
		let group = tg::group::Data {
			id,
			name,
			parent: parent.cloned(),
			specifier: specifier.clone(),
		};

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn create_group_request(
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
		let output = self.create_group(session, arg).await?;
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
