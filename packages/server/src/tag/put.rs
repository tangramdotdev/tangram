use {
	crate::{Session, context::Authentication, database::Database, database::Transaction},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
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
				self.try_put_tag_local(tag, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, %tag, "failed to put the tag"))?;
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
		let grant_creator_admin = self.authorize_put_tag(tag).await?;

		// Insert the tag into the database unless this is a replicated request.
		if !arg.replicate {
			match &self.server.database {
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					self.put_tag_postgres(database, tag, &arg, grant_creator_admin)
						.await
						.map_err(|error| tg::error!(!error, "failed to put the tag"))?;
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					self.put_tag_sqlite(database, tag, &arg, grant_creator_admin)
						.await
						.map_err(|error| tg::error!(!error, "failed to put the tag"))?;
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
			.map_err(|error| tg::error!(!error, "failed to index the tag"))?;

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
						|error| tg::error!(!error, %tag, "failed to put the tag in another region"),
					)?;
			}
		}

		Ok(())
	}

	pub(super) async fn authorize_put_tag(&self, tag: &tg::Tag) -> tg::Result<bool> {
		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let grant_creator_admin = self
			.authorize_put_tag_with_transaction(&transaction, tag)
			.await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(grant_creator_admin)
	}

	pub(super) async fn authorize_put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		if Self::try_get_tag_namespace_id_with_transaction(transaction, tag)
			.await?
			.is_some()
		{
			match &self.context.authentication {
				Some(Authentication::Root) => return Ok(false),
				Some(Authentication::User(user))
					if Self::user_has_tag_permission_with_transaction(
						transaction,
						&user.id,
						tag,
						tg::Permission::Write,
					)
					.await? =>
				{
					return Ok(false);
				},
				_ => {},
			}
			return Err(tg::error!("unauthorized"));
		}

		if Self::try_get_namespace_id_with_transaction(transaction, &tag_to_namespace(tag))
			.await?
			.is_some()
		{
			return Err(tg::error!("a namespace exists at the tag path"));
		}

		if tag.namespace.is_root() {
			return match &self.context.authentication {
				Some(Authentication::Root | Authentication::User(_)) => Ok(true),
				_ => Err(tg::error!("unauthorized")),
			};
		}

		if Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace)
			.await?
			.is_some()
		{
			match &self.context.authentication {
				Some(Authentication::Root) => Ok(true),
				Some(Authentication::User(user)) => {
					if Self::user_has_namespace_permission_with_transaction(
						transaction,
						&user.id,
						&tag.namespace,
						tg::Permission::Write,
					)
					.await?
					{
						Ok(true)
					} else {
						Err(tg::error!("unauthorized"))
					}
				},
				_ => Err(tg::error!("unauthorized")),
			}
		} else {
			self.create_missing_tag_parent_namespaces_with_transaction(transaction, &tag.namespace)
				.await?;
			Ok(true)
		}
	}

	async fn create_missing_tag_parent_namespaces_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<()> {
		let user = match &self.context.authentication {
			Some(Authentication::Root) => None,
			Some(Authentication::User(user)) => Some(user.id.clone()),
			_ => return Err(tg::error!("unauthorized")),
		};
		let mut components = Vec::new();
		let mut parent_created = false;
		for component in namespace.components() {
			components.push(component.to_owned());
			let current = tg::Namespace::with_components(components.clone());
			if Self::try_get_namespace_id_with_transaction(transaction, &current)
				.await?
				.is_some()
			{
				parent_created = false;
				continue;
			}
			if Self::namespace_path_has_tag_with_transaction(transaction, &current).await? {
				return Err(tg::error!("a tag exists at the namespace path"));
			}
			if components.len() > 1 && !parent_created {
				let parent = tg::Namespace::with_components(
					components[..components.len() - 1].iter().cloned(),
				);
				match &self.context.authentication {
					Some(Authentication::Root) => {},
					Some(Authentication::User(user))
						if Self::user_has_namespace_permission_with_transaction(
							transaction,
							&user.id,
							&parent,
							tg::Permission::Write,
						)
						.await? => {},
					_ => return Err(tg::error!("unauthorized")),
				}
			}
			let namespace_id =
				Self::get_or_create_namespace_with_transaction(transaction, &current).await?;
			if let Some(user) = user.as_ref() {
				Self::create_namespace_grant_for_user_with_transaction(
					transaction,
					&current,
					namespace_id,
					user,
					tg::Permission::Admin,
					Some(user),
				)
				.await?;
			}
			parent_created = true;
		}
		Ok(())
	}

	async fn try_put_tag_region(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, %tag, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::tag::put::Arg {
			location: Some(location.into()),
			..arg
		};
		client
			.put_tag(tag, arg)
			.await
			.map_err(|error| tg::error!(!error, %tag, region = %region, "failed to put the tag"))?;
		Ok(())
	}

	async fn try_put_tag_remote(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, %tag, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::tag::put::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			replicate: false,
			..arg
		};
		client
			.put_tag(tag, arg)
			.await
			.map_err(|error| tg::error!(!error, %tag, remote = %remote, "failed to put the tag"))?;
		Ok(())
	}

	pub(crate) async fn put_tag_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let mut arg: tg::tag::put::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let tag = arg.tag.take().ok_or_else(|| tg::error!("expected a tag"))?;

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

fn tag_to_namespace(tag: &tg::Tag) -> tg::Namespace {
	tg::Namespace::with_components(tag.components().map(ToOwned::to_owned))
}
