use {
	crate::{Session, context::Authentication, database::Database, database::Transaction},
	futures::{FutureExt as _, TryStreamExt as _, stream::FuturesUnordered},
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
	tangram_object_store::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

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
		let grant_creator_admin = self.authorize_put_tag(tag, &arg.item).await?;

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
				#[cfg(feature = "turso")]
				Database::Turso(database) => {
					self.put_tag_turso(database, tag, &arg, grant_creator_admin)
						.await
						.map_err(|error| tg::error!(!error, "failed to put the tag"))?;
				},
			}
		}

		// Insert the tag into the index.
		self.server
			.index
			.put_tags(&[tangram_index::PutTagArg {
				tag: tag.clone(),
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

	pub(super) async fn authorize_put_tag_item_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		item: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<()> {
		let tg::Either::Left(id) = item else {
			return Ok(());
		};
		let principal = self.read_principal();
		let arg = crate::object::store::TryGetArg {
			id: id.clone(),
			now: time::OffsetDateTime::now_utc().unix_timestamp(),
			principal: principal.clone(),
		};
		let output = self
			.server
			.object_store
			.try_get(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		if output.object.is_some() && Self::authorize_object(&principal, &output, true) {
			return Ok(());
		}
		if Self::tag_points_to_object_with_transaction(transaction, tag, id).await? {
			return Ok(());
		}
		if output.object.is_none() {
			return Err(tg::error!("object not found"));
		}
		Err(tg::error!("object not found"))
	}

	pub(super) async fn authorize_put_tag_item_batch_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		items: &[tg::tag::batch::Item],
	) -> tg::Result<()> {
		let object_items = items
			.iter()
			.filter_map(|item| {
				let tg::Either::Left(id) = &item.item else {
					return None;
				};
				Some((&item.tag, id))
			})
			.collect::<Vec<_>>();
		if object_items.is_empty() {
			return Ok(());
		}

		let principal = self.read_principal();
		let arg = crate::object::store::TryGetBatchArg {
			ids: object_items.iter().map(|(_, id)| (*id).clone()).collect(),
			now: time::OffsetDateTime::now_utc().unix_timestamp(),
			principal: principal.clone(),
		};
		let outputs = self
			.server
			.object_store
			.try_get_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the objects"))?;
		let mut fallback_items = Vec::new();
		for ((tag, id), output) in std::iter::zip(object_items, outputs) {
			if output.object.is_some() && Self::authorize_object(&principal, &output, true) {
				continue;
			}
			fallback_items.push((tag, id));
		}
		let fallback_results =
			Self::tag_points_to_object_batch_with_transaction(transaction, &fallback_items).await?;
		for authorized in fallback_results {
			if authorized {
				continue;
			}
			return Err(tg::error!("object not found"));
		}

		Ok(())
	}

	async fn tag_points_to_object_with_transaction(
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		id: &tg::object::Id,
	) -> tg::Result<bool> {
		let results =
			Self::tag_points_to_object_batch_with_transaction(transaction, &[(tag, id)]).await?;
		Ok(results.into_iter().next().unwrap_or_default())
	}

	async fn tag_points_to_object_batch_with_transaction(
		transaction: &Transaction<'_>,
		items: &[(&tg::Tag, &tg::object::Id)],
	) -> tg::Result<Vec<bool>> {
		if items.is_empty() {
			return Ok(Vec::new());
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			namespace: String,
			name: String,

			#[tangram_database(as = "db::value::FromStr")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}

		let p = transaction.p();
		let mut params = Vec::with_capacity(items.len() * 2);
		let mut conditions = Vec::with_capacity(items.len());
		for (tag, _) in items {
			let namespace = tag.namespace.to_string();
			let name = tag.name.to_string();
			let namespace_param = format!("{p}{}", params.len() + 1);
			params.push(db::value::Serialize::serialize(&namespace).unwrap());
			let name_param = format!("{p}{}", params.len() + 1);
			params.push(db::value::Serialize::serialize(&name).unwrap());
			conditions.push(format!(
				"(coalesce(namespaces.name, '') = {namespace_param} and tags.name = {name_param})"
			));
		}
		let statement = format!(
			"
				select coalesce(namespaces.name, '') as namespace, tags.name, tags.item
				from tags
				left join namespaces on tags.namespace = namespaces.id
				where {};
			",
			conditions.join(" or ")
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		let rows = rows
			.into_iter()
			.map(|row| ((row.namespace, row.name), row.item))
			.collect::<BTreeMap<_, _>>();
		let results = items
			.iter()
			.map(|(tag, id)| {
				rows.get(&(tag.namespace.to_string(), tag.name.to_string()))
					.is_some_and(|item| item == &tg::Either::Left((*id).clone()))
			})
			.collect();
		Ok(results)
	}

	pub(super) async fn authorize_put_tag(
		&self,
		tag: &tg::Tag,
		item: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<bool> {
		let item = item.clone();
		let session = self.clone();
		let tag = tag.clone();
		self.server
			.database
			.run(|transaction| {
				let item = item.clone();
				let session = session.clone();
				let tag = tag.clone();
				async move {
					session
						.authorize_put_tag_inner_with_transaction(transaction, &tag, &item)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the tag"))
	}

	async fn authorize_put_tag_inner_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
		item: &tg::Either<tg::object::Id, tg::process::Id>,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		let grant_creator_admin = match self
			.authorize_put_tag_with_transaction(transaction, tag)
			.await?
		{
			ControlFlow::Break(grant_creator_admin) => grant_creator_admin,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		self.authorize_put_tag_item_with_transaction(transaction, tag, item)
			.await?;
		Ok(ControlFlow::Break(grant_creator_admin))
	}

	pub(super) async fn authorize_put_tag_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		tag: &tg::Tag,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		if Self::try_get_tag_namespace_id_with_transaction(transaction, tag)
			.await?
			.is_some()
		{
			match &self.context.authentication {
				Some(Authentication::Root) => return Ok(ControlFlow::Break(false)),
				Some(Authentication::User(user))
					if Self::user_has_tag_permission_with_transaction(
						transaction,
						&user.id,
						tag,
						tg::Permission::Write,
					)
					.await? =>
				{
					return Ok(ControlFlow::Break(false));
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
				Some(Authentication::Root | Authentication::User(_)) => {
					Ok(ControlFlow::Break(true))
				},
				_ => Err(tg::error!("unauthorized")),
			};
		}

		if Self::try_get_namespace_id_with_transaction(transaction, &tag.namespace)
			.await?
			.is_some()
		{
			match &self.context.authentication {
				Some(Authentication::Root) => Ok(ControlFlow::Break(true)),
				Some(Authentication::User(user)) => {
					if Self::user_has_namespace_permission_with_transaction(
						transaction,
						&user.id,
						&tag.namespace,
						tg::Permission::Write,
					)
					.await?
					{
						Ok(ControlFlow::Break(true))
					} else {
						Err(tg::error!("unauthorized"))
					}
				},
				_ => Err(tg::error!("unauthorized")),
			}
		} else {
			match self
				.create_missing_tag_parent_namespaces_with_transaction(transaction, &tag.namespace)
				.await?
			{
				ControlFlow::Break(()) => Ok(ControlFlow::Break(true)),
				ControlFlow::Continue(error) => Ok(ControlFlow::Continue(error)),
			}
		}
	}

	async fn create_missing_tag_parent_namespaces_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
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
			let namespace_id = match Self::get_or_create_namespace_with_transaction(
				transaction,
				&current,
			)
			.await?
			{
				ControlFlow::Break(namespace_id) => namespace_id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if let Some(user) = user.as_ref() {
				match Self::create_namespace_grant_for_user_with_transaction(
					transaction,
					&current,
					namespace_id,
					user,
					tg::Permission::Admin,
					Some(user),
				)
				.await?
				{
					ControlFlow::Break(_) => {},
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
			parent_created = true;
		}
		Ok(ControlFlow::Break(()))
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
