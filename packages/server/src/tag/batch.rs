use {
	crate::{Database, Session, context::Authentication},
	futures::{FutureExt as _, TryStreamExt as _},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

impl Session {
	pub(crate) async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;

		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.post_tag_batch_local(&arg).await?;
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.post_tag_batch_region(arg, region).await?;
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.post_tag_batch_remote(arg, remote, region).await?;
			},
		}

		Ok(())
	}

	async fn post_tag_batch_local(&self, arg: &tg::tag::batch::Arg) -> tg::Result<()> {
		// Authorize.
		let grant_creator_admin = self.authorize_tag_batch(arg).await?;
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());

		// Insert the tags into the database unless this is a replicated request.
		if !arg.replicate {
			match &self.server.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					self.post_tag_batch_postgres(
						database,
						arg,
						created_by.as_ref(),
						&grant_creator_admin,
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to post the tag batch"))?;
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					self.post_tag_batch_sqlite(
						database,
						arg,
						created_by.as_ref(),
						&grant_creator_admin,
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to post the tag batch"))?;
				},
				#[cfg(feature = "turso")]
				Database::Turso(database) => {
					self.post_tag_batch_turso(
						database,
						arg,
						created_by.as_ref(),
						&grant_creator_admin,
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to post the tag batch"))?;
				},
			}
		}

		// Insert the tags into the index.
		let put_tag_args: Vec<_> = arg
			.tags
			.iter()
			.map(|item| tangram_index::PutTagArg {
				tag: item.tag.clone(),
				item: item.item.clone(),
			})
			.collect();
		self.server
			.index
			.put_tags(&put_tag_args)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the tags"))?;

		// Handle regions unless this is a replicated request.
		if !arg.replicate {
			let location = tg::Location::Local(tg::location::Local::default()).into();
			let locations = self
				.locations(Some(&location))
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
			if let Some(local) = locations.local
				&& !local.regions.is_empty()
			{
				local
					.regions
					.iter()
					.map(|region| {
						let arg = tg::tag::batch::Arg {
							replicate: true,
							..arg.clone()
						};
						self.post_tag_batch_region(arg, region.clone())
					})
					.collect::<futures::stream::FuturesUnordered<_>>()
					.try_collect::<()>()
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to post the tag batch in another region")
					})?;
			}
		}

		Ok(())
	}

	async fn authorize_tag_batch(&self, arg: &tg::tag::batch::Arg) -> tg::Result<Vec<bool>> {
		let arg = arg.clone();
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					session
						.authorize_tag_batch_with_transaction(transaction, &arg)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the tag batch"))
	}

	async fn authorize_tag_batch_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::tag::batch::Arg,
	) -> tg::Result<ControlFlow<Vec<bool>, crate::database::Error>> {
		let mut grant_creator_admin = Vec::with_capacity(arg.tags.len());
		for item in &arg.tags {
			let value = match self
				.authorize_put_tag_with_transaction(transaction, &item.tag)
				.await?
			{
				ControlFlow::Break(value) => value,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			grant_creator_admin.push(value);
		}
		self.authorize_put_tag_item_batch_with_transaction(transaction, &arg.tags)
			.await?;
		Ok(ControlFlow::Break(grant_creator_admin))
	}

	async fn post_tag_batch_region(
		&self,
		arg: tg::tag::batch::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::tag::batch::Arg {
			location: Some(location.into()),
			..arg
		};
		client.post_tag_batch(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to post the tag batch"),
		)?;
		Ok(())
	}

	async fn post_tag_batch_remote(
		&self,
		arg: tg::tag::batch::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<()> {
		let client = self
			.get_remote_session(&remote)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote client"))?;
		let arg = tg::tag::batch::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			replicate: false,
			..arg
		};
		client
			.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to post the tag batch on the remote"))?;
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
		self.post_tag_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to post the tag batch"))?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
