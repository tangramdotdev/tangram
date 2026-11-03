use {
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_server_core::{Session, list::QuerySpecifierItemsArg},
	tangram_server_list_common::{Kinds, item_kind_enabled, sort_items},
};

#[cfg(test)]
mod tests;

pub mod remote;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	remote_tasks: self::remote::Tasks,
}

#[derive(Default, serde::Deserialize)]
struct Arg {
	#[serde(flatten)]
	arg: tg::list::Arg,

	#[serde(flatten)]
	options: tg::referent::Options,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { position: u64 },
}

#[derive(Clone)]
enum Parent {
	Any,
	Id(tg::Id),
	Root,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		let remote_tasks = self::remote::Tasks::default();
		let state = State { remote_tasks };

		Self(Arc::new(state))
	}

	#[tracing::instrument(level = "trace", name = "list", skip_all)]
	pub(crate) async fn list(
		&self,
		session: &Session,
		mut arg: tg::list::Arg,
	) -> tg::Result<tg::list::Output> {
		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let cursor = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;
		let position = cursor.map_or(0, |cursor| match cursor {
			Cursor::V0 { position } => position,
		});

		// Collect the complete result before selecting the page.
		arg.cursor = None;
		arg.limit = None;
		let mut data = self.list_inner(session, arg).await?;

		// Select the page and create the continuation cursor.
		let position = usize::try_from(position)
			.unwrap_or(usize::MAX)
			.min(data.len());
		let end = position
			.saturating_add(usize::try_from(limit).unwrap())
			.min(data.len());
		let cursor = if end < data.len() {
			let cursor = Cursor::V0 {
				position: u64::try_from(end).unwrap(),
			};
			Some(tangram_server_core::cursor::serialize(&cursor)?)
		} else {
			None
		};
		let data = data.drain(position..end).collect();
		let output = tg::list::Output { cursor, data };

		Ok(output)
	}

	pub async fn list_inner(
		&self,
		session: &Session,
		mut arg: tg::list::Arg,
	) -> tg::Result<Vec<tg::list::Item>> {
		session.verify_request_with_network_access()?;
		if let Some(location) = arg
			.node
			.as_ref()
			.and_then(|node| node.options.location.clone())
		{
			arg.location = Some(location.into());
		}
		let local_only =
			arg.location
				.as_ref()
				.and_then(tg::location::Arg::to_location)
				.is_some_and(|location| match location {
					tg::Location::Local(local) => local.region.as_deref().is_none_or(|region| {
						Some(region) == session.server.config.region.as_deref()
					}),
					tg::Location::Remote(_) => false,
				});
		if local_only && !arg.recursive {
			let data = self.list_local_items_for_list(session, &arg).await?;

			return Ok(sort_items(data, arg.reverse));
		}
		let local_arg = arg.clone();
		let tokens = arg
			.node
			.as_ref()
			.map(tg::Referent::options)
			.map(|options| options.tokens.clone())
			.unwrap_or_default();
		let query_arg = QuerySpecifierItemsArg {
			cached: arg.cached,
			filter_local: move |items| {
				let data = filter_list_items(items, &local_arg);
				sort_items(data, local_arg.reverse)
			},
			location: arg.location.as_ref(),
			query: tangram_server_core::list::remote::Query::List(arg.clone()),
			tokens: &tokens,
			ttl: arg.ttl,
		};
		let items = self.query_specifier_items(session, query_arg).await?;
		let data = sort_items(items, arg.reverse);

		Ok(data)
	}

	pub(crate) async fn list_local_items(
		&self,
		session: &Session,
	) -> tg::Result<Vec<tg::list::Item>> {
		self.list_local_items_with_tokens(session, &tg::authorization::Tokens::default())
			.await
	}

	async fn list_local_items_with_tokens(
		&self,
		session: &Session,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Vec<tg::list::Item>> {
		// List the items.
		let items = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				async move { Self::list_local_items_with_transaction(transaction).await }.boxed()
			})
			.await?;

		// Filter the visible items.
		let items = self.filter_visible_items(session, items, tokens).await?;

		Ok(items)
	}

	pub(crate) async fn list_local_items_for_list(
		&self,
		session: &Session,
		arg: &tg::list::Arg,
	) -> tg::Result<Vec<tg::list::Item>> {
		// Resolve the parent.
		let parent = match &arg.node {
			None => Parent::Root,
			Some(node) => Parent::Id(node.node.clone()),
		};

		// Page through the database items.
		let mut database_position = 0;
		let mut output = Vec::new();
		let tokens = arg
			.node
			.as_ref()
			.map(tg::Referent::options)
			.map(|options| options.tokens.clone())
			.unwrap_or_default();
		loop {
			let database_length = session.server.config.pagination.database_page_length;
			let arg = arg.clone();
			let parent = parent.clone();
			let items = session
				.server
				.database
				.run_with_options(db::ConnectionOptions::default(), |transaction| {
					let arg = arg.clone();
					let parent = parent.clone();
					async move {
						Self::list_local_items_for_list_with_transaction(
							transaction,
							&arg,
							&parent,
							database_position,
							database_length,
						)
						.await
					}
					.boxed()
				})
				.await?;
			let input_length = items.len().to_u64().unwrap();
			let items = self.filter_visible_items(session, items, &tokens).await?;
			output.extend(items);
			if input_length < database_length {
				break;
			}
			database_position = database_position.saturating_add(input_length);
		}

		Ok(output)
	}

	async fn list_local_items_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		let mut items = Vec::new();
		let groups = match Self::list_local_groups(transaction, &Parent::Any).await? {
			ControlFlow::Break(groups) => groups,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		items.extend(groups);
		let organizations = match Self::list_local_organizations(transaction).await? {
			ControlFlow::Break(organizations) => organizations,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		items.extend(organizations);
		let tags = match Self::list_local_tags(transaction, &Parent::Any).await? {
			ControlFlow::Break(tags) => tags,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		items.extend(tags);
		let users = match Self::list_local_users(transaction).await? {
			ControlFlow::Break(users) => users,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		items.extend(users);

		Ok(ControlFlow::Break(items))
	}

	async fn list_local_items_for_list_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: &tg::list::Arg,
		parent: &Parent,
		position: u64,
		length: u64,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
			target: Option<String>,
		}

		// Build the query.
		let p = transaction.p();
		let condition = |table: &str| match parent {
			Parent::Any => "true".to_owned(),
			Parent::Id(_) => format!("{table}.parent = {p}1"),
			Parent::Root => format!("{table}.parent is null"),
		};
		let mut queries = Vec::new();
		if arg.groups {
			let condition = condition("groups");
			queries.push(format!(
				"
					select groups.id, groups.parent, specifiers.specifier,
						cast(null as text) as target
					from groups
					join specifiers on specifiers.id = groups.id
					where {condition}
				"
			));
		}
		if arg.organizations && matches!(parent, Parent::Root) {
			queries.push(
				"
					select organizations.id, cast(null as text) as parent,
						specifiers.specifier, cast(null as text) as target
					from organizations
					join specifiers on specifiers.id = organizations.id
				"
				.to_owned(),
			);
		}
		if arg.tags {
			let condition = condition("tags");
			queries.push(format!(
				"
					select tags.id, tags.parent, specifiers.specifier, tags.target
					from tags
					join specifiers on specifiers.id = tags.id
					where {condition}
				"
			));
		}
		if arg.users && matches!(parent, Parent::Root) {
			queries.push(
				"
					select users.id, cast(null as text) as parent,
						specifiers.specifier, cast(null as text) as target
					from users
					join specifiers on specifiers.id = users.id
				"
				.to_owned(),
			);
		}
		if queries.is_empty() {
			return Ok(ControlFlow::Break(Vec::new()));
		}
		let direction = if arg.reverse { "desc" } else { "asc" };
		let mut params = match parent {
			Parent::Id(parent) => db::params![parent.to_string()],
			Parent::Any | Parent::Root => db::params![],
		};
		let length_parameter = params.len() + 1;
		let position_parameter = params.len() + 2;
		params.extend(db::params![
			length.to_i64().unwrap_or(i64::MAX),
			position.to_i64().unwrap_or(i64::MAX)
		]);
		let statement = format!(
			"
				select id, parent, specifier, target
				from ({}) as items
				order by specifier {direction}
				limit {p}{length_parameter}
				offset {p}{position_parameter};
			",
			queries.join(" union all ")
		);

		// Execute the query.
		let result = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");

		// Create the items.
		let location = tg::Location::Local(tg::location::Local::default());
		let mut items = Vec::with_capacity(rows.len());
		for row in rows {
			let id = row.id;
			let target = match id.kind() {
				tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User => None,
				tg::id::Kind::Tag => {
					let target = row
						.target
						.ok_or_else(|| tg::error!("expected a tag target"))?;
					let target = tangram_server_tag_common::parse_tag_target(&target)?;
					let target = Self::tag_target_referent(target, Some(location.clone()));
					Some(target)
				},
				_ => return Err(tg::error!(%id, "invalid list item")),
			};
			let options = tg::referent::Options {
				location: Some(location.clone()),
				..Default::default()
			};
			let node = tg::Referent::new(id.clone(), options);
			let item = tg::list::Item {
				node,
				parent: row.parent,
				specifier: row.specifier,
				target,
			};
			items.push((id, item));
		}

		Ok(ControlFlow::Break(items))
	}

	pub async fn query_specifier_items<F>(
		&self,
		session: &Session,
		arg: QuerySpecifierItemsArg<'_, F>,
	) -> tg::Result<Vec<tg::list::Item>>
	where
		F: FnOnce(Vec<tg::list::Item>) -> Vec<tg::list::Item> + Send,
	{
		let QuerySpecifierItemsArg {
			cached,
			filter_local,
			location,
			query,
			tokens,
			ttl,
		} = arg;
		let locations = session
			.locations(location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let mut sources = Vec::new();
		if locations.local.is_some() {
			let items = self
				.list_local_items_with_tokens(session, tokens)
				.await
				.map_err(|error| tg::error!(!error, "failed to list the local items"))?;
			let items = filter_local(items);
			sources.push(items);
		}
		let mut remotes = locations.remotes;
		remotes.sort_by(|a, b| a.name.cmp(&b.name));
		let remote_results = remotes
			.into_iter()
			.map(|remote| {
				let query = query.clone();
				async move {
					let name = remote.name.clone();
					let data = self
						.list_remote(session, remote.clone(), cached, ttl, query)
						.await
						.map_err(
							|error| tg::error!(!error, remote = %name, "failed to query the remote items"),
						)?;
					Ok::<_, tg::Error>((name, data))
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let mut remote_results = remote_results;
		remote_results.sort_by(|a, b| a.0.cmp(&b.0));
		sources.extend(remote_results.into_iter().map(|(_, items)| items));
		let items = merge_items(sources);

		Ok(items)
	}

	async fn filter_visible_items(
		&self,
		session: &Session,
		items: Vec<(tg::Id, tg::list::Item)>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Vec<tg::list::Item>> {
		if items.is_empty() {
			return Ok(Vec::new());
		}
		let ids = items.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
		let visible = session
			.server
			.index
			.visible(&ids, &session.context.principal)
			.await?;
		let mut output = Vec::new();
		for ((id, mut item), visible) in std::iter::zip(items, visible) {
			let authorized = if visible {
				true
			} else {
				let permission = tangram_server_grant_common::read_permission_for_resource(&id)?;
				let resource = tg::Referent::with_node_and_tokens(
					tg::Selector::Id(id.clone()),
					tokens.clone(),
				);
				session
					.authorize(resource, permission)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(permission)
			};
			if authorized {
				let tokens = tg::authorization::Tokens::with_authorization(
					session
						.server
						.components
						.authorization
						.create_read_token(session, &id)?,
				);
				if let Some(target) = &mut item.target {
					target.options.tokens = tokens.clone();
				}
				item.set_tokens(tokens);
				output.push(item);
			}
		}

		Ok(output)
	}

	async fn list_local_groups(
		transaction: &tangram_server_core::database::Transaction<'_>,
		parent: &Parent,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::group::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let p = transaction.p();
		let (statement, params) = match parent {
			Parent::Any => (
				"
					select groups.id, groups.parent, specifiers.specifier
					from groups
					join specifiers on specifiers.id = groups.id
					order by specifiers.specifier;
				"
				.to_owned(),
				db::params![],
			),
			Parent::Id(parent) => (
				format!(
					"
						select groups.id, groups.parent, specifiers.specifier
						from groups
						join specifiers on specifiers.id = groups.id
						where groups.parent = {p}1
						order by specifiers.specifier;
					"
				),
				db::params![parent.to_string()],
			),
			Parent::Root => (
				"
					select groups.id, groups.parent, specifiers.specifier
					from groups
					join specifiers on specifiers.id = groups.id
					where groups.parent is null
					order by specifiers.specifier;
				"
				.to_owned(),
				db::params![],
			),
		};
		let result = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let items = rows
			.into_iter()
			.map(|row| {
				let id: tg::Id = row.id.clone().into();
				let location = tg::Location::Local(tg::location::Local::default());
				let options = tg::referent::Options {
					location: Some(location),
					..Default::default()
				};
				let node = tg::Referent::new(id.clone(), options);
				let item = tg::list::Item {
					node,
					parent: row.parent,
					specifier: row.specifier,
					target: None,
				};
				(id, item)
			})
			.collect();

		Ok(ControlFlow::Break(items))
	}

	async fn list_local_organizations(
		transaction: &tangram_server_core::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::organization::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let result = transaction
			.query_all_into::<Row>(
				"
					select organizations.id, specifiers.specifier
					from organizations
					join specifiers on specifiers.id = organizations.id
					order by specifiers.specifier;
				"
				.into(),
				db::params![],
			)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let items = rows
			.into_iter()
			.map(|row| {
				let id: tg::Id = row.id.clone().into();
				let location = tg::Location::Local(tg::location::Local::default());
				let options = tg::referent::Options {
					location: Some(location),
					..Default::default()
				};
				let node = tg::Referent::new(id.clone(), options);
				let item = tg::list::Item {
					node,
					parent: None,
					specifier: row.specifier,
					target: None,
				};
				(id, item)
			})
			.collect();

		Ok(ControlFlow::Break(items))
	}

	async fn list_local_tags(
		transaction: &tangram_server_core::database::Transaction<'_>,
		parent: &Parent,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::tag::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
			target: String,
		}
		let p = transaction.p();
		let (statement, params) = match parent {
			Parent::Any => (
				"
					select tags.id, tags.parent, specifiers.specifier, tags.target
					from tags
					join specifiers on specifiers.id = tags.id
					order by specifiers.specifier;
				"
				.to_owned(),
				db::params![],
			),
			Parent::Id(parent) => (
				format!(
					"
						select tags.id, tags.parent, specifiers.specifier, tags.target
						from tags
						join specifiers on specifiers.id = tags.id
						where tags.parent = {p}1
						order by specifiers.specifier;
					"
				),
				db::params![parent.to_string()],
			),
			Parent::Root => (
				"
					select tags.id, tags.parent, specifiers.specifier, tags.target
					from tags
					join specifiers on specifiers.id = tags.id
					where tags.parent is null
					order by specifiers.specifier;
				"
				.to_owned(),
				db::params![],
			),
		};
		let result = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let mut items = Vec::new();
		for row in rows {
			let target = tangram_server_tag_common::parse_tag_target(&row.target)?;
			let location = tg::Location::Local(tg::location::Local::default());
			let id: tg::Id = row.id.clone().into();
			let options = tg::referent::Options {
				location: Some(location.clone()),
				..Default::default()
			};
			let node = tg::Referent::new(id.clone(), options);
			let target = Some(Self::tag_target_referent(target, Some(location)));
			let item = tg::list::Item {
				node,
				parent: row.parent,
				specifier: row.specifier,
				target,
			};
			items.push((id, item));
		}

		Ok(ControlFlow::Break(items))
	}

	#[must_use]
	fn tag_target_referent(
		target: tg::tag::data::Target,
		location: Option<tg::Location>,
	) -> tg::Referent<tg::Either<tg::object::Id, tg::process::Id>> {
		let target = match target {
			tg::tag::data::Target::Object(id) => tg::Either::Left(id),
			tg::tag::data::Target::Process(id) => tg::Either::Right(id),
		};
		let options = tg::referent::Options {
			location,
			..tg::referent::Options::default()
		};

		tg::Referent::new(target, options)
	}

	async fn list_local_users(
		transaction: &tangram_server_core::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let result = transaction
			.query_all_into::<Row>(
				"
					select users.id, specifiers.specifier
					from users
					join specifiers on specifiers.id = users.id
					order by specifiers.specifier;
				"
				.into(),
				db::params![],
			)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let items = rows
			.into_iter()
			.map(|row| {
				let id: tg::Id = row.id.clone().into();
				let location = tg::Location::Local(tg::location::Local::default());
				let options = tg::referent::Options {
					location: Some(location),
					..Default::default()
				};
				let node = tg::Referent::new(id.clone(), options);
				let item = tg::list::Item {
					node,
					parent: None,
					specifier: row.specifier,
					target: None,
				};
				(id, item)
			})
			.collect();

		Ok(ControlFlow::Break(items))
	}

	pub(crate) async fn list_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg::<Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let Arg {
			mut arg,
			mut options,
		} = arg.unwrap_or_default();
		if !path.is_empty() {
			let id = path
				.join("/")
				.parse()
				.map_err(|error| tg::error!(argument, !error, "failed to parse the list node"))?;
			// The location applies to both the list and its node.
			options.location = arg
				.location
				.as_ref()
				.map(|location| {
					location
						.to_location()
						.ok_or_else(|| tg::error!(argument, "expected a single node location"))
				})
				.transpose()?;
			arg.node = Some(tg::Referent::new(id, options));
		}
		let output = self.list(session, arg).await?;
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::GET, ["list", path @ ..]) => {
				self.list_request(session, request, path).await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl Default for Component {
	fn default() -> Self {
		Self::new()
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn stop<'a>(&'a self, _server: &'a tangram_server_core::Server) -> BoxFuture<'a, ()> {
		async move {
			// Abort the remote tasks.
			self.remote_tasks.abort_all();
			let results = self.remote_tasks.wait().await;
			for result in results {
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "a remote list task panicked");
				}
			}
			tracing::trace!("remote list tasks");
		}
		.boxed()
	}

	fn abort(&self) {
		self.remote_tasks.abort_all();
	}
}

impl tangram_server_core::list::Component for Component {
	fn query_specifier_items<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: QuerySpecifierItemsArg<'a>,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Item>>> {
		self.query_specifier_items(session, arg).boxed()
	}

	fn list<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::list::Output>> {
		self.list(session, arg).boxed()
	}

	fn list_inner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Item>>> {
		self.list_inner(session, arg).boxed()
	}

	fn list_local_items<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Item>>> {
		self.list_local_items(session).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}

fn filter_list_items(items: Vec<tg::list::Item>, arg: &tg::list::Arg) -> Vec<tg::list::Item> {
	let kinds = Kinds {
		groups: arg.groups,
		organizations: arg.organizations,
		tags: arg.tags,
		users: arg.users,
	};
	let parent = arg.node.as_ref().map(|node| node.node.clone());
	let descendants = if arg.recursive {
		let mut descendants = BTreeSet::new();
		if let Some(parent) = &parent {
			descendants.insert(parent.clone());
		}
		loop {
			let length = descendants.len();
			for item in &items {
				let include = match (parent.as_ref(), item.parent()) {
					(None, _) => true,
					(Some(_), Some(parent)) => descendants.contains(parent),
					(Some(_), None) => false,
				};
				if include {
					descendants.insert(item.id().clone());
				}
			}
			if descendants.len() == length {
				break;
			}
		}
		Some(descendants)
	} else {
		None
	};
	items
		.into_iter()
		.filter(|item| {
			let matches_parent = if let Some(descendants) = &descendants {
				(parent.is_none() || descendants.contains(item.id()))
					&& parent.as_ref() != Some(item.id())
			} else {
				item.parent() == parent.as_ref()
			};
			matches_parent && item_kind_enabled(item, &kinds)
		})
		.collect()
}

fn merge_items(sources: Vec<Vec<tg::list::Item>>) -> Vec<tg::list::Item> {
	let mut emitted = BTreeSet::new();
	let mut output = Vec::new();
	for items in sources {
		output.extend(
			items
				.into_iter()
				.filter(|item| emitted.insert(item.specifier().clone())),
		);
	}

	output
}
