use {
	crate::{Context, Database, Server},
	futures::TryStreamExt as _,
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

impl Server {
	pub(crate) async fn post_tag_batch_with_context(
		&self,
		context: &Context,
		arg: tg::tag::batch::Arg,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self
			.location(arg.location.as_ref())
			.map_err(|source| tg::error!(!source, "failed to resolve the location"))?;

		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.post_tag_batch_local(context, &arg).await?;
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

	async fn post_tag_batch_local(
		&self,
		context: &Context,
		arg: &tg::tag::batch::Arg,
	) -> tg::Result<()> {
		// Authorize.
		self.authorize(context)
			.await
			.map_err(|source| tg::error!(!source, "failed to authorize"))?;

		// Insert the tags into the database unless this is a replicated request.
		if !arg.replicate {
			match &self.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					Self::post_tag_batch_postgres(database, arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to post the tag batch"))?;
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					Self::post_tag_batch_sqlite(database, arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to post the tag batch"))?;
				},
			}
		}

		// Insert the tags into the index.
		let put_tag_args: Vec<_> = arg
			.tags
			.iter()
			.map(|item| tangram_index::PutTagArg {
				tag: item.tag.to_string(),
				item: item.item.clone(),
			})
			.collect();
		self.index
			.put_tags(&put_tag_args)
			.await
			.map_err(|source| tg::error!(!source, "failed to index the tags"))?;

		// Handle regions unless this is a replicated request.
		if !arg.replicate {
			let location = tg::Location::Local(tg::location::Local::default()).into();
			let locations = self
				.locations(Some(&location))
				.await
				.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;
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
					.map_err(|source| {
						tg::error!(!source, "failed to post the tag batch in another region")
					})?;
			}
		}

		Ok(())
	}

	async fn post_tag_batch_region(
		&self,
		arg: tg::tag::batch::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::tag::batch::Arg {
			location: Some(location.into()),
			..arg
		};
		client.post_tag_batch(arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to post the tag batch"),
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
			.get_remote_client(remote)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
		let arg = tg::tag::batch::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			replicate: false,
			..arg
		};
		client
			.post_tag_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post the tag batch on the remote"))?;
		Ok(())
	}

	pub(crate) async fn handle_post_tag_batch_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		self.post_tag_batch_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post the tag batch"))?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
