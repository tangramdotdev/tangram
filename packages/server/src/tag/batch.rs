use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
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
			tg::Location::Local(_) => self.post_tag_batch_local(arg).await,
			tg::Location::Remote(remote) => self.post_tag_batch_remote(arg, remote).await,
		}
	}

	async fn post_tag_batch_local(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let permission = tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		);
		let permissions = tg::authorization::permission::Set::from_permission(permission);
		let authorizations = arg
			.tags
			.iter()
			.map(|item| {
				(
					tg::Selector::<tg::Id>::Specifier(item.specifier.clone()),
					permissions,
				)
			})
			.collect::<Vec<_>>();
		let authorized = self.authorize_batch(authorizations).await?;
		if authorized.into_iter().any(|permissions| {
			permissions.is_some_and(|permissions| !permissions.contains(permission))
		}) {
			return Err(tg::error!("unauthorized"));
		}
		let mut permissions = Vec::with_capacity(arg.tags.len());
		for item in &arg.tags {
			permissions.push(self.recorded_tag_target_permissions(&item.target).await?);
		}
		let touched_at = self.server.clock.unix_timestamp()?;
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let permissions = permissions.clone();
				let session = session.clone();
				async move {
					session
						.post_tag_batch_local_with_transaction(
							transaction,
							arg,
							permissions,
							touched_at,
						)
						.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		self.checkout_index_barrier().await?;
		Ok(())
	}

	async fn post_tag_batch_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::tag::batch::Arg,
		permissions: Vec<Vec<tg::authorization::Permission>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		for (item, permissions) in std::iter::zip(arg.tags, permissions) {
			let arg = tg::tag::put::Arg {
				ancestors: tg::node::Ancestors {
					create: arg.parents,
					pull: tg::node::AncestorsPull::Never,
				},
				location: None,
				public: false,
				specifier: item.specifier,
				target: item.target,
			};
			let data = match self
				.put_tag_with_transaction(transaction, arg, permissions, &mut batch)
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
			let target = match data.target {
				tg::tag::data::Target::Object(id) => tg::Either::Left(id),
				tg::tag::data::Target::Process(id) => tg::Either::Right(id),
			};
			batch.items.push(tangram_index::batch::Item::PutTag(
				tangram_index::tag::put::Arg {
					account: account.clone(),
					id: data.id,
					name: data.name,
					parent: data.parent,
					permissions: data.permissions,
					specifier: data.specifier,
					target: target.clone(),
				},
			));
			if let Some(account) = account {
				let item = match target {
					tg::Either::Left(object) => tangram_index::batch::Item::PutAccountObject(
						tangram_index::usage::storage::put::ObjectArg {
							account,
							object,
							touched_at,
						},
					),
					tg::Either::Right(process) => tangram_index::batch::Item::PutAccountProcess(
						tangram_index::usage::storage::put::ProcessArg {
							account,
							process,
							touched_at,
						},
					),
				};
				batch.items.push(item);
			}
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

	async fn post_tag_batch_remote(
		&self,
		mut arg: tg::tag::batch::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
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
