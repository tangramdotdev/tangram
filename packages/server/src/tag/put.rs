use {
	crate::{Session, database::Database},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
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

impl Session {
	pub(crate) async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|source| tg::error!(!source, "failed to resolve the location"))?;

		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_put_tag_local(tag, arg.clone())
					.await
					.map_err(|source| tg::error!(!source, %tag, "failed to put the tag"))?;
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_put_tag_region(tag, arg, region).await?;
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_put_tag_remote(tag, arg, remote, region).await?;
			},
		}

		Ok(())
	}

	async fn try_put_tag_local(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		// Authorize.
		self.authorize()
			.await
			.map_err(|source| tg::error!(!source, "failed to authorize"))?;

		// Insert the tag into the database unless this is a replicated request.
		if !arg.replicate {
			match &self.server.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					self.put_tag_postgres(database, tag, &arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to put the tag"))?;
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					self.put_tag_sqlite(database, tag, &arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to put the tag"))?;
				},
			}
		}

		// Insert the tag into the index.
		self.server
			.index
			.put_tags(&[tangram_index::PutTagArg {
				tag: tag.to_string(),
				item: arg.item.clone(),
			}])
			.await
			.map_err(|source| tg::error!(!source, "failed to index the tag"))?;

		// Handle regions unless this is a replicated request.
		if !arg.replicate {
			let location = tg::Location::Local(tg::location::Local::default()).into();
			let locations = self
				.server
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
						let arg = tg::tag::put::Arg {
							replicate: true,
							..arg.clone()
						};
						self.try_put_tag_region(tag, arg, region.clone())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await
					.map_err(
						|source| tg::error!(!source, %tag, "failed to put the tag in another region"),
					)?;
			}
		}

		Ok(())
	}

	async fn try_put_tag_region(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_session(region.clone()).await.map_err(
			|source| tg::error!(!source, %tag, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::tag::put::Arg {
			location: Some(location.into()),
			..arg
		};
		client.put_tag(tag, arg).await.map_err(
			|source| tg::error!(!source, %tag, region = %region, "failed to put the tag"),
		)?;
		Ok(())
	}

	async fn try_put_tag_remote(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<()> {
		let client = self.get_remote_session(remote.clone()).await.map_err(
			|source| tg::error!(!source, %tag, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::tag::put::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			replicate: false,
			..arg
		};
		client.put_tag(tag, arg).await.map_err(
			|source| tg::error!(!source, %tag, remote = %remote, "failed to put the tag"),
		)?;
		Ok(())
	}

	pub(crate) async fn put_tag_request(
		&self,
		request: http::Request<BoxBody>,
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
		self.put_tag(&tag, arg).await?;

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
