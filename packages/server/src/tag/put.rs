use {
	crate::{Context, Server, database::Database},
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn try_put_tag_with_context(
		&self,
		context: &Context,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> tg::Result<Option<()>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let locations = self
			.locations_with_regions(arg.locations.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_put_tag_local(context, tag, arg.clone())
					.await
					.map_err(|source| tg::error!(!source, %tag, "failed to put the tag"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_put_tag_from_regions(tag, arg.clone(), &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %tag, "failed to put the tag in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_put_tag_from_remotes(tag, arg, &locations.remotes)
			.await
			.map_err(|source| tg::error!(!source, %tag, "failed to put the tag in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_put_tag_local(
		&self,
		context: &Context,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> tg::Result<Option<()>> {
		// Authorize.
		self.authorize(context)
			.await
			.map_err(|source| tg::error!(!source, "failed to authorize"))?;

		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let Some(()) = (match &arg.item {
			tg::Either::Left(id) => self
				.index
				.touch_object(id, touched_at)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to touch the object"))?
				.map(|_| ()),
			tg::Either::Right(id) => self
				.index
				.touch_process(id, touched_at)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to touch the process"))?
				.map(|_| ()),
		}) else {
			return Ok(None);
		};

		// Insert the tag into the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_tag_postgres(database, tag, &arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the tag"))?;
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				Self::put_tag_sqlite(database, tag, &arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the tag"))?;
			},
		}

		// Index the tag.
		self.index
			.put_tags(&[tangram_index::PutTagArg {
				tag: tag.to_string(),
				item: arg.item,
			}])
			.await
			.map_err(|source| tg::error!(!source, "failed to index the tag"))?;

		Ok(Some(()))
	}

	async fn try_put_tag_from_regions(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_put_tag_from_region(tag, arg.clone(), region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_put_tag_from_region(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, %tag, region = %region, "failed to get the region client"),
		)?;
		let arg = tg::tag::put::Arg {
			force: arg.force,
			item: arg.item,
			locations: tg::location::Locations {
				local: Some(tg::Either::Right(tg::location::Local {
					regions: Some(vec![region.to_owned()]),
				})),
				remotes: Some(tg::Either::Left(false)),
			},
		};
		let Some(()) = client.try_put_tag(tag, arg).await.map_err(
			|source| tg::error!(!source, %tag, region = %region, "failed to put the tag"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_put_tag_from_remotes(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		remotes: &[tg::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_put_tag_from_remote(tag, arg.clone(), remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_put_tag_from_remote(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		remote: &tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, %tag, remote = %remote.remote, "failed to get the remote client"),
			)?;
		let arg = tg::tag::put::Arg {
			force: arg.force,
			item: arg.item,
			locations: tg::location::Locations {
				local: match &remote.regions {
					Some(regions) => Some(tg::Either::Right(tg::location::Local {
						regions: Some(regions.clone()),
					})),
					None => Some(tg::Either::Left(true)),
				},
				remotes: Some(tg::Either::Left(false)),
			},
		};
		let Some(()) = client.try_put_tag(tag, arg).await.map_err(
			|source| tg::error!(!source, %tag, remote = %remote.remote, "failed to put the tag"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_put_tag_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		tag: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the tag.
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Put the tag.
		let Some(()) = self.try_put_tag_with_context(context, &tag, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
