use {
	crate::Session,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Session {
	pub(crate) async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.verify_request_with_network_access()?;
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				self.post_tag_batch_region(arg, region).await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !self.server.is_primary_region() =>
			{
				self.post_tag_batch_primary_region(arg).await
			},
			tg::Location::Local(_) => self.post_tag_batch_local(arg).await,
			tg::Location::Remote(remote) => self.post_tag_batch_remote(arg, remote).await,
		}
	}

	async fn post_tag_batch_local(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let specifiers = arg
			.tags
			.iter()
			.map(|item| item.specifier.clone())
			.collect::<Vec<_>>();
		let touched_at = self.server.clock.unix_timestamp()?;
		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let session = session.clone();
			let specifiers = specifiers.clone();
			async move {
				match session
					.post_tag_batch_local_attempt(arg, &specifiers, touched_at)
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
		self.server
			.spawn_publish_database_index_queue_notification_task();
		Ok(())
	}

	async fn post_tag_batch_local_attempt(
		&self,
		arg: tg::tag::batch::Arg,
		specifiers: &[tg::Specifier],
		touched_at: i64,
	) -> tg::Result<ControlFlow<()>> {
		let ids_by_specifier = self
			.try_get_ids_and_ancestors_for_specifiers(specifiers)
			.await?;
		self.authorize_tag_puts(specifiers, arg.force, &ids_by_specifier)
			.await?;
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let ids_by_specifier = ids_by_specifier.clone();
				let session = session.clone();
				async move {
					session
						.post_tag_batch_local_with_transaction(
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

		Ok(output)
	}

	async fn post_tag_batch_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::tag::batch::Arg,
		ids_by_specifier: BTreeMap<tg::Specifier, Option<tg::Id>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<ControlFlow<()>, crate::database::Error>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
		match Self::verify_ids_for_specifiers_with_transaction(
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
		let mut batch = tangram_index::batch::Arg::default();
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
				.put_tag_with_transaction(transaction, arg, &mut batch)
				.await?
			{
				ControlFlow::Break(data) => data,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let account = match self
				.usage_account_for_specifier_with_transaction(transaction, &data.specifier)
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
			batch.items.push(tangram_index::batch::Item::PutTag(
				tangram_index::tag::put::Arg {
					touched_at,
					account: account.clone(),
					id: data.id,
					name: data.name,
					parent: data.parent,
					specifier: data.specifier,
					target: target.clone(),
					version: version.clone(),
				},
			));
			let resource = tg::Referent::with_node_and_tokens(
				match &target {
					tg::Either::Left(id) => tg::Id::from(id.clone()),
					tg::Either::Right(id) => tg::Id::from(id.clone()),
				},
				tokens,
			);
			let capture = self.create_capture_permissions_batch_items(
				destination,
				Some(version),
				[resource],
				self.context.principal.clone(),
				touched_at,
			)?;
			batch.items.extend(capture);
		}
		match self
			.server
			.enqueue_database_index_queue_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(ControlFlow::Break(())))
	}

	async fn post_tag_batch_primary_region(&self, mut arg: tg::tag::batch::Arg) -> tg::Result<()> {
		let client = self
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
		mut arg: tg::tag::batch::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_session(&region).await.map_err(
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
		mut arg: tg::tag::batch::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
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
		self.invalidate_remote_cache(&remote.name).await;

		Ok(())
	}

	pub(crate) async fn post_tag_batch_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.post_tag_batch(arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
