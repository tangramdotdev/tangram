use {
	crate::{Context, Server, database::Database},
	futures::TryStreamExt as _,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn delete_tags_with_context(
		&self,
		context: &Context,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self
			.location(arg.location.as_ref())
			.map_err(|source| tg::error!(!source, "failed to resolve the location"))?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.delete_tags_local(context, arg.clone()).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.delete_tags_region(arg, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.delete_tags_remote(arg, remote, region).await?,
		};

		Ok(output)
	}

	async fn delete_tags_local(
		&self,
		context: &Context,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		// Authorize.
		self.authorize(context)
			.await
			.map_err(|source| tg::error!(!source, "failed to authorize"))?;

		let output = if arg.replicate.is_none() {
			// Delete the tags from the database.
			match &self.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					Self::delete_tags_postgres(database, &arg.pattern, arg.recursive)
						.await
						.map_err(|source| tg::error!(!source, "failed to delete the tag"))?
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => Self::delete_tags_sqlite(database, &arg.pattern, arg.recursive)
					.await
					.map_err(|source| tg::error!(!source, "failed to delete the tag"))?,
			}
		} else {
			let deleted = arg
				.replicate
				.clone()
				.ok_or_else(|| tg::error!("expected deleted tags for a replicated delete"))?;
			tg::tag::delete::Output { deleted }
		};

		// Delete the tags from the index.
		let tags = output
			.deleted
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		if !tags.is_empty() {
			self.index
				.delete_tags(&tags)
				.await
				.map_err(|source| tg::error!(!source, "failed to index the deleted tags"))?;
		}

		// Handle regions if this is the primary delete request.
		if arg.replicate.is_none() {
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
						let arg = tg::tag::delete::Arg {
							replicate: Some(output.deleted.clone()),
							..arg.clone()
						};
						async move {
							self.delete_tags_region(arg, region.clone())
								.await
								.map(|_| ())
						}
					})
					.collect::<futures::stream::FuturesUnordered<_>>()
					.try_collect::<()>()
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to delete the tag in another region")
					})?;
			}
		}

		Ok(output)
	}

	async fn delete_tags_region(
		&self,
		arg: tg::tag::delete::Arg,
		region: String,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::tag::delete::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.delete_tags(arg)
			.await
			.map_err(|source| tg::error!(!source, region = %region, "failed to delete the tag"))?;
		Ok(output)
	}

	async fn delete_tags_remote(
		&self,
		arg: tg::tag::delete::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self
			.get_remote_client(remote)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
		let arg = tg::tag::delete::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			replicate: None,
			..arg
		};
		let output = client
			.delete_tags(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the tag on remote"))?;
		Ok(output)
	}

	pub(crate) async fn handle_delete_tag_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		_tag: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Delete the tag.
		let output = self.delete_tags_with_context(context, arg).await?;

		// Create the response.
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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
