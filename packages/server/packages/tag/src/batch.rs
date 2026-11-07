use {
	crate::Component,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn post_tag_batch(
		&self,
		session: &Session,
		arg: tg::tag::batch::Arg,
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
				self.post_tag_batch_region(session, arg, region).await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.post_tag_batch_primary_region(session, arg).await
			},
			tg::Location::Local(_) => self.post_tag_batch_local(session, arg).await,
			tg::Location::Remote(remote) => self.post_tag_batch_remote(session, arg, remote).await,
		}
	}

	async fn post_tag_batch_local(
		&self,
		session: &Session,
		arg: tg::tag::batch::Arg,
	) -> tg::Result<()> {
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let specifiers = arg
			.tags
			.iter()
			.map(|item| item.specifier.clone())
			.collect::<Vec<_>>();
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
			let specifiers = specifiers.clone();
			async move {
				match component
					.post_tag_batch_local_attempt(&session, arg, &specifiers, touched_at)
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

	async fn post_tag_batch_local_attempt(
		&self,
		session: &Session,
		arg: tg::tag::batch::Arg,
		specifiers: &[tg::Specifier],
		touched_at: i64,
	) -> tg::Result<ControlFlow<()>> {
		let ids_by_specifier = session
			.server
			.components
			.node
			.try_get_ids_and_ancestors_for_specifiers(session, specifiers)
			.await?;
		self.authorize_tag_puts(session, specifiers, arg.force, &ids_by_specifier)
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
						.post_tag_batch_local_with_transaction(
							&session,
							transaction,
							arg,
							ids_by_specifier,
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

	async fn post_tag_batch_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::tag::batch::Arg,
		ids_by_specifier: BTreeMap<tg::Specifier, Option<tg::Id>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<ControlFlow<Vec<tg::user::Id>>, tangram_server_core::database::Error>>
	{
		let batch_size = session.server.config.sync.get.database.batch_size;
		match tangram_server_node_common::specifier::verify_ids_for_specifiers_with_transaction(
			transaction,
			&ids_by_specifier,
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
		for item in arg.tags {
			let arg = tg::tag::put::Arg {
				tokens: item.tokens,
				ancestors: tg::node::Ancestors {
					create: arg.parents,
					pull: tg::node::AncestorsPull::Never,
				},
				force: arg.force,
				location: None,
				public: false,
				specifier: item.specifier,
				target: item.target,
			};
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

		let users = tangram_server_authentication_common::deleted_users(&batch);

		Ok(ControlFlow::Break(ControlFlow::Break(users)))
	}

	async fn post_tag_batch_primary_region(
		&self,
		session: &Session,
		mut arg: tg::tag::batch::Arg,
	) -> tg::Result<()> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the tags in the primary region"))?;

		Ok(())
	}

	async fn post_tag_batch_region(
		&self,
		session: &Session,
		mut arg: tg::tag::batch::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		for item in &mut arg.tags {
			item.tokens = item
				.tokens
				.for_location(&tg::Location::Local(tg::location::Local {
					region: Some(region.clone()),
				}));
		}
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		client
			.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to put the tags"))?;

		Ok(())
	}

	async fn post_tag_batch_remote(
		&self,
		session: &Session,
		mut arg: tg::tag::batch::Arg,
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
		for item in &mut arg.tags {
			item.tokens = item
				.tokens
				.for_location(&tg::Location::Remote(remote.clone()));
		}
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		client
			.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to put the tags"))?;
		session
			.server
			.components
			.remote
			.invalidate_remote_cache(session, &remote.name)
			.await;

		Ok(())
	}

	pub(crate) async fn post_tag_batch_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.post_tag_batch(session, arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
