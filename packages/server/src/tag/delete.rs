use {
	crate::{Session, context::Authentication, database::Database},
	futures::TryStreamExt as _,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Session {
	pub(crate) async fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
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

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.delete_tags_local(arg.clone()).await?
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
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		// Authorize.
		self.authorize_delete_tags(&arg).await?;

		let output = if arg.replicate.is_none() {
			// Delete the tags from the database.
			match &self.server.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => self
					.delete_tags_postgres(database, &arg.pattern, arg.recursive)
					.await
					.map_err(|error| tg::error!(!error, "failed to delete the tag"))?,
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => self
					.delete_tags_sqlite(database, &arg.pattern, arg.recursive)
					.await
					.map_err(|error| tg::error!(!error, "failed to delete the tag"))?,
			}
		} else {
			let deleted = arg
				.replicate
				.clone()
				.ok_or_else(|| tg::error!("expected deleted tags for a replicated delete"))?;
			tg::tag::delete::Output { deleted }
		};

		// Delete the tags from the index.
		let tags = output.deleted.clone();
		if !tags.is_empty() {
			self.server
				.index
				.delete_tags(&tags)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the deleted tags"))?;
		}

		// Handle regions if this is the primary delete request.
		if arg.replicate.is_none() {
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
					.map_err(|error| {
						tg::error!(!error, "failed to delete the tag in another region")
					})?;
			}
		}

		Ok(output)
	}

	async fn authorize_delete_tags(&self, arg: &tg::tag::delete::Arg) -> tg::Result<()> {
		if arg.pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
		}
		if !arg.recursive && arg.pattern.contains_operators() {
			return Err(tg::error!(
				"cannot delete multiple tags without --recursive"
			));
		}

		let user = match &self.context.authentication {
			Some(Authentication::Root) => return Ok(()),
			Some(Authentication::User(user)) => user,
			_ => return Err(tg::error!("unauthorized")),
		};

		let tags = if let Some(deleted) = &arg.replicate {
			deleted.clone()
		} else {
			let output = self
				.list_local(tg::list::Arg {
					cached: false,
					length: None,
					location: None,
					namespaces: false,
					pattern: arg.pattern.clone(),
					recursive: arg.recursive,
					reverse: false,
					tags: true,
					ttl: None,
				})
				.await
				.map_err(|error| tg::error!(!error, "failed to list the tags"))?;
			output
				.data
				.into_iter()
				.filter_map(|entry| match entry {
					tg::list::Entry::Tag { tag, .. } => Some(tag),
					tg::list::Entry::Namespace { .. } => None,
				})
				.collect()
		};
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for tag in tags {
			if !Self::user_has_tag_permission_with_transaction(
				&transaction,
				&user.id,
				&tag,
				tg::Permission::Write,
			)
			.await?
			{
				return Err(tg::error!("unauthorized"));
			}
		}
		Ok(())
	}

	async fn delete_tags_region(
		&self,
		arg: tg::tag::delete::Arg,
		region: String,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
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
			.map_err(|error| tg::error!(!error, region = %region, "failed to delete the tag"))?;
		Ok(output)
	}

	async fn delete_tags_remote(
		&self,
		arg: tg::tag::delete::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self
			.get_remote_session(&remote)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote client"))?;
		let arg = tg::tag::delete::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			replicate: None,
			..arg
		};
		let output = client
			.delete_tags(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the tag on remote"))?;
		Ok(output)
	}

	pub(crate) async fn delete_tags_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Delete the tag.
		let output = self.delete_tags(arg).await?;

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
