use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_server_core::{Session, database::Transaction},
};

impl Component {
	pub(crate) async fn put_tag(
		&self,
		session: &Session,
		arg: tg::tag::put::Arg,
	) -> tg::Result<()> {
		session.verify_request_with_network_access()?;
		let location = session
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.put_tag_region(session, arg, region).await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.put_tag_primary_region(session, arg).await
			},
			tg::Location::Local(_) => self.put_tag_local(session, arg).await,
			tg::Location::Remote(remote) => self.put_tag_remote(session, arg, remote).await,
		}
	}

	async fn put_tag_local(&self, session: &Session, arg: tg::tag::put::Arg) -> tg::Result<()> {
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		session
			.server
			.components
			.node
			.pull_ancestors(session, &arg.specifier, arg.ancestors.pull)
			.await?;
		let touched_at = session.server.clock.unix_timestamp();
		let options = session
			.server
			.config
			.verification
			.write_retry
			.clone()
			.into();
		let component = self.clone();
		let session = session.clone();
		tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let component = component.clone();
			let session = session.clone();
			async move {
				match component
					.put_tag_local_attempt(&session, arg, touched_at)
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
		Ok(())
	}

	async fn put_tag_local_attempt(
		&self,
		session: &Session,
		arg: tg::tag::put::Arg,
		touched_at: i64,
	) -> tg::Result<ControlFlow<()>> {
		let specifiers = std::slice::from_ref(&arg.specifier);
		let ids_by_specifier = session
			.server
			.components
			.node
			.try_get_ids_and_ancestors_for_specifiers(session, specifiers)
			.await?;
		self.authorize_tag_puts(session, specifiers, arg.force, &ids_by_specifier)
			.await?;
		tangram_server_core::checkpoint!(session.server, "tag.put.authorized", specifier = %arg.specifier).await;
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
						.put_tag_local_with_transaction(
							&session,
							transaction,
							arg,
							&ids_by_specifier,
							touched_at,
						)
						.await
				}
				.boxed()
			})
			.await?;

		let users = match output {
			ControlFlow::Break(users) => users,
			ControlFlow::Continue(()) => return Ok(ControlFlow::Continue(())),
		};
		session
			.server
			.components
			.authentication
			.invalidate_user_authentication(&session.server, &users)
			.await;

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn authorize_tag_puts(
		&self,
		session: &Session,
		specifiers: &[tg::Specifier],
		force: bool,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<()> {
		let batch_size = session.server.config.sync.get.database.batch_size;

		// Collect the required write and replacement authorizations.
		let mut authorizations = Vec::new();
		for specifier in specifiers {
			let existing = ids_by_specifier
				.get(specifier)
				.and_then(std::option::Option::as_ref);
			let parent = specifier
				.parent()
				.and_then(|specifier| ids_by_specifier.get(&specifier))
				.and_then(std::option::Option::as_ref);
			match existing {
				Some(id) if id.kind() == tg::id::Kind::Tag => {
					let permission = tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Write,
					);
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Id(id.clone());
					authorizations.push((resource, false, permissions));
				},
				Some(id) => {
					if !force {
						return Err(tg::error!("specifier is already in use"));
					}
					let permission =
						tangram_server_node_common::delete_permission_for_named_node(id)?;
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Id(id.clone());
					authorizations.push((resource, false, permissions));
					if let Some(parent) = parent {
						let permission =
							tangram_server_grant_common::write_permission_for_resource(parent)?;
						let permissions =
							tg::authorization::permission::Set::from_permission(permission);
						let resource = tg::Selector::<tg::Id>::Id(parent.clone());
						authorizations.push((resource, false, permissions));
					}
				},
				None => {
					let permission = tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Write,
					);
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Specifier(specifier.clone());
					authorizations.push((resource, true, permissions));
				},
			}
		}

		// Authorize the operations in batches.
		for authorizations in authorizations.chunks(batch_size) {
			let args = authorizations
				.iter()
				.map(|(resource, _, permissions)| (resource.clone(), *permissions))
				.collect::<Vec<_>>();
			let outputs = session.authorize_batch(args, None).await?;
			tangram_server_core::authorization::check_exhaustion(&outputs)?;
			for ((resource, allow_unclaimed, permissions), output) in
				std::iter::zip(authorizations, outputs)
			{
				let authorized = output.permissions.contains(*permissions)
					|| (*allow_unclaimed
						&& match resource {
							tg::Selector::Specifier(specifier) => {
								std::iter::once(specifier.clone())
									.chain(specifier.ancestors())
									.all(|specifier| {
										ids_by_specifier
											.get(&specifier)
											.is_some_and(Option::is_none)
									})
							},
							tg::Selector::Id(_) => false,
						});
				if !authorized {
					return Err(tg::error!("unauthorized"));
				}
			}
		}

		Ok(())
	}

	async fn put_tag_local_with_transaction(
		&self,
		session: &Session,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<ControlFlow<Vec<tg::user::Id>>, tangram_server_core::database::Error>>
	{
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
		let mut batch = tangram_index::write::Arg::default();
		let tokens = arg.tokens.clone();
		let (data, version) = match self
			.put_tag_with_transaction(session, transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(data) => data,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let account = match session
			.server
			.components
			.usage
			.usage_account_for_specifier_with_transaction(session, transaction, &data.specifier)
			.await?
		{
			ControlFlow::Break(account) => account,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let destination = data.id.clone().into();
		let target = match data.target {
			tg::tag::data::Target::Object(id) => tg::Either::Left(id),
			tg::tag::data::Target::Process(id) => tg::Either::Right(id),
		};
		let resource = tg::Referent::with_node_and_tokens(
			match &target {
				tg::Either::Left(id) => tg::Id::from(id.clone()),
				tg::Either::Right(id) => tg::Id::from(id.clone()),
			},
			tokens,
		);
		let captures = session.server.components.authorization.create_captures(
			session,
			destination,
			Some(version.clone()),
			vec![resource],
			session.context.principal.clone(),
			touched_at,
		)?;
		let arg = tangram_index::tag::put::Arg {
			account: account.clone(),
			captures,
			id: data.id,
			name: data.name,
			parent: data.parent,
			specifier: data.specifier,
			target: target.clone(),
			touched_at,
			version,
		};
		batch.items.push(tangram_index::Item::PutTag(Box::new(arg)));
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

		let users = tangram_server_authentication_common::deleted_users(&batch);

		Ok(ControlFlow::Break(ControlFlow::Break(users)))
	}

	pub(crate) async fn put_tag_with_transaction(
		&self,
		session: &Session,
		transaction: &Transaction<'_>,
		arg: tg::tag::put::Arg,
		batch: &mut tangram_index::write::Arg,
	) -> tg::Result<ControlFlow<(tg::tag::Data, String), tangram_server_core::database::Error>> {
		let existing =
			match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
				transaction,
				&arg.specifier,
			)
			.await?
			{
				ControlFlow::Break(existing) => existing,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};

		// Delete a conflicting non-tag node and its subtree.
		let existing = if existing
			.as_ref()
			.is_some_and(|existing| existing.kind() != tg::id::Kind::Tag)
		{
			if !arg.force {
				return Err(tg::error!("specifier is already in use"));
			}
			let existing = existing
				.as_ref()
				.ok_or_else(|| tg::error!("missing the existing named node"))?;
			let batch_size = session.server.config.sync.get.database.batch_size;
			let replaced_ids_and_specifiers = match session
				.server
				.components
				.node
				.collect_named_subtrees_with_transaction(
					transaction,
					std::slice::from_ref(existing),
					batch_size,
				)
				.await?
			{
				ControlFlow::Break(ids_and_specifiers) => ids_and_specifiers,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			match session
				.server
				.components
				.node
				.delete_named_nodes_with_transaction(
					session,
					transaction,
					&replaced_ids_and_specifiers,
					batch,
					batch_size,
				)
				.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			None
		} else {
			existing
		};

		// Resolve the parent.
		let parent = if arg.ancestors.create {
			match session
				.server
				.components
				.group
				.create_parent_groups_with_transaction(session, transaction, &arg.specifier, batch)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		} else {
			match session
				.server
				.components
				.group
				.resolve_parent_for_specifier_with_transaction(transaction, &arg.specifier)
				.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		};

		// Put the tag.
		let target = Self::tag_target_to_string(&arg.target);
		let (id, version) = if let Some(id) = existing {
			let id =
				tg::tag::Id::try_from(id).map_err(|_| tg::error!("specifier is already in use"))?;
			#[derive(db::row::Deserialize)]
			struct Row {
				target: String,
				version: String,
			}
			let p = transaction.p();
			let statement = formatdoc!(
				"
					select target, version
					from tags
					where id = {p}1;
				"
			);
			let result = transaction
				.query_one_into::<Row>(statement.into(), db::params![id.to_string()])
				.await;
			let row =
				tangram_server_core::database::retry!(result, "failed to execute the statement");
			let version = if row.target == target {
				row.version
			} else {
				tangram_server_tag_common::version()
			};
			if row.target != target && !arg.force {
				return Err(tg::error!("the tag already has a different target"));
			}
			let p = transaction.p();
			let statement = formatdoc!(
				"
					update tags
					set target = {p}1, version = {p}3
					where id = {p}2;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![target.clone(), id.to_string(), version.clone()],
				)
				.await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
			(id, version)
		} else {
			let id = tg::tag::Id::new();
			let version = tangram_server_tag_common::version();
			match tangram_server_node_common::specifier::insert_specifier_with_transaction(
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
					insert into tags (id, name, parent, target, version)
					values ({p}1, {p}2, {p}3, {p}4, {p}5);
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						id.to_string(),
						name,
						parent.as_ref().map(ToString::to_string),
						target.clone(),
						version.clone(),
					],
				)
				.await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
			if arg.public {
				let arg = tg::grant::create::Arg {
					subject: tg::authorization::Subject::Public.into(),
					permissions: tg::Either::Left(
						tg::authorization::Permission::Tag(
							tg::authorization::permission::tag::Permission::Read,
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
			if let Some(subject) = Self::write_user_grant_subject(session) {
				let arg = tg::grant::create::Arg {
					subject: subject.into(),
					permissions: tg::Either::Left(
						tg::authorization::Permission::Tag(
							tg::authorization::permission::tag::Permission::Admin,
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
			(id, version)
		};
		let data = tg::tag::Data {
			id,
			name: arg.specifier.name().to_owned(),
			parent,
			specifier: arg.specifier,
			target: arg.target,
		};

		Ok(ControlFlow::Break((data, version)))
	}

	async fn put_tag_primary_region(
		&self,
		session: &Session,
		mut arg: tg::tag::put::Arg,
	) -> tg::Result<()> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the tag in the primary region"))?;

		Ok(())
	}

	async fn put_tag_region(
		&self,
		session: &Session,
		mut arg: tg::tag::put::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.tokens = arg
			.tokens
			.for_location(&tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			}));
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to put the tag"))?;

		Ok(())
	}

	async fn put_tag_remote(
		&self,
		session: &Session,
		mut arg: tg::tag::put::Arg,
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
		arg.tokens = arg
			.tokens
			.for_location(&tg::Location::Remote(remote.clone()));
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to put the tag"))?;
		session
			.server
			.components
			.remote
			.invalidate_remote_cache(session, &remote.name)
			.await;

		Ok(())
	}

	pub(crate) async fn put_tag_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.put_tag(session, arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) fn write_user_grant_subject(
		session: &Session,
	) -> Option<tg::authorization::Subject> {
		match &session.context.principal {
			tg::Principal::User(user) => Some(tg::authorization::Subject::User(user.clone())),
			_ => None,
		}
	}
}
