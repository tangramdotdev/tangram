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
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}
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
		for item in &arg.tags {
			if let Some(permission) = self.write_permission_for_specifier(&item.specifier).await? {
				let authorized = self
					.authorize(
						tg::grant::Resource::Specifier(item.specifier.clone()),
						permission,
					)
					.await?;
				if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
					return Err(tg::error!("unauthorized"));
				}
			}
		}
		let mut permissions = Vec::with_capacity(arg.tags.len());
		for item in &arg.tags {
			permissions.push(self.recorded_tag_permissions(&item.item).await?);
		}
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let permissions = permissions.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					for (item, permissions) in std::iter::zip(arg.tags, permissions) {
						let arg = tg::tag::put::Arg {
							force: item.force,
							item: item.item,
							location: None,
							public: false,
							specifier: item.specifier,
						};
						let data = session
							.put_tag_with_transaction(transaction, arg, permissions, &mut batch)
							.await?;
						batch.items.push(tangram_index::batch::Item::PutTag(
							tangram_index::tag::put::Arg {
								id: data.id,
								item: match data.item {
									tg::tag::data::Item::Object(id) => tg::Either::Left(id),
									tg::tag::data::Item::Process(id) => tg::Either::Right(id),
								},
								name: data.name,
								parent: data.parent,
								permissions: data.permissions,
								specifier: data.specifier,
							},
						));
					}
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;
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
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to put the tags"))?;
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
