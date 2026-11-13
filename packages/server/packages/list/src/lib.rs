use {
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream::FuturesOrdered},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_server_core::{
		Session,
		list::{Cursor, Page, PageArg, Scope},
	},
	tangram_server_list_common::{Kinds, item_kind_enabled},
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
		session.verify_request_with_network_access()?;

		// List from the node's location.
		if let Some(location) = arg
			.node
			.as_ref()
			.and_then(|node| node.options.location.clone())
		{
			arg.location = Some(location.into());
		}

		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let limit = usize::try_from(limit).unwrap();
		let after = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?
			.map(|cursor| match cursor {
				Cursor::V0 { after } => after,
			});

		// Resolve the scope, which is the node's children, or its descendants when the list is recursive.
		let scope = match (&arg.node, arg.recursive) {
			(None, false) => Scope::Children(None),
			(None, true) => Scope::All,
			(Some(node), false) => Scope::Children(Some(tg::Selector::Id(node.node.clone()))),
			(Some(node), true) => match self.try_get_local_specifier(session, &node.node).await? {
				Some(specifier) => Scope::Descendants(specifier),
				None => Scope::Empty,
			},
		};

		// List the page.
		let kinds = Kinds {
			groups: arg.groups,
			organizations: arg.organizations,
			tags: arg.tags,
			users: arg.users,
		};
		let tokens = arg
			.node
			.as_ref()
			.map_or_default(|node| node.options.tokens.clone());
		let page_arg = PageArg {
			after,
			cached: arg.cached,
			filter: Box::new(move |item| item_kind_enabled(item, &kinds)),
			limit,
			location: arg.location.as_ref(),
			query: tangram_server_core::list::remote::Query::List(arg.clone()),
			reverse: arg.reverse,
			scope,
			time_to_live: arg.time_to_live,
			tokens: &tokens,
		};
		let page = self.list_page(session, page_arg).await?;

		// Create the output.
		let cursor = page
			.next
			.map(|after| Cursor::V0 { after })
			.as_ref()
			.map(tangram_server_core::cursor::serialize)
			.transpose()?;
		let output = tg::list::Output {
			cursor,
			data: page.data,
		};

		Ok(output)
	}

	pub async fn list_page(&self, session: &Session, arg: PageArg<'_>) -> tg::Result<Page> {
		let locations = session
			.locations(arg.location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		// List a page of the local items.
		let mut pages = Vec::new();
		if locations.local.is_some() {
			let page = self
				.list_local_page(session, &arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to list the local items"))?;
			pages.push(page);
		}

		// List a page of each remote's items, in the order of the remotes' names.
		let mut remotes = locations.remotes;
		remotes.sort_by(|a, b| a.name.cmp(&b.name));
		let query = query_with_page(arg.query.clone(), arg.after.clone(), arg.limit)?;
		let remote_pages = remotes
			.into_iter()
			.map(|remote| {
				let query = query.clone();
				async move {
					let name = remote.name.clone();
					self.list_remote(session, remote, arg.cached, arg.time_to_live, query)
						.await
						.map_err(
							|error| tg::error!(!error, remote = %name, "failed to query the remote items"),
						)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		pages.extend(remote_pages);

		// Merge the pages.
		let page = merge_pages(pages, arg.limit, arg.reverse);

		Ok(page)
	}

	async fn list_local_page(&self, session: &Session, arg: &PageArg<'_>) -> tg::Result<Page> {
		if matches!(arg.scope, Scope::Empty) {
			let page = Page {
				data: Vec::new(),
				next: None,
			};
			return Ok(page);
		}

		// Read the scope's items in batches, after the key, until one more item than the limit passes the filters or the scope is exhausted.
		let batch_size = u64::try_from(arg.limit).unwrap() + 1;
		let reverse = arg.reverse;
		let mut after = arg.after.clone();
		let mut data = Vec::new();
		loop {
			let rows = session
				.server
				.database
				.run_with_options(db::ConnectionOptions::default(), |transaction| {
					let after = after.clone();
					let scope = arg.scope.clone();
					async move {
						Self::list_local_page_with_transaction(
							transaction,
							&scope,
							after.as_deref(),
							reverse,
							batch_size,
						)
						.await
					}
					.boxed()
				})
				.await?;
			let exhausted = rows.len().to_u64().unwrap() < batch_size;
			if let Some((key, _, _)) = rows.last() {
				after = Some(key.clone());
			}
			let items = rows
				.into_iter()
				.map(|(_, id, item)| (id, item))
				.filter(|(_, item)| (arg.filter)(item))
				.collect();
			let items = self
				.filter_visible_items(session, items, arg.tokens)
				.await?;
			data.extend(items);
			if data.len() > arg.limit || exhausted {
				break;
			}
		}

		// Create the page, which continues after its last item when more items passed the filters.
		let more = data.len() > arg.limit;
		data.truncate(arg.limit);
		let next = more
			.then(|| data.last().map(|item| item.specifier().key()))
			.flatten();
		let page = Page { data, next };

		Ok(page)
	}

	async fn list_local_page_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		scope: &Scope,
		after: Option<&[u8]>,
		reverse: bool,
		length: u64,
	) -> tg::Result<
		ControlFlow<Vec<(Vec<u8>, tg::Id, tg::list::Item)>, tangram_server_core::database::Error>,
	> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,

			key: bytes::Bytes,

			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,

			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,

			target: Option<String>,
		}

		// Build the condition for the scope and the key.
		let p = transaction.p();
		let mut conditions = Vec::new();
		let mut params = Vec::new();
		match scope {
			Scope::All | Scope::Empty => (),
			Scope::Children(None) => {
				conditions.push("coalesce(specifiers.parent, '') = ''".to_owned());
			},
			Scope::Children(Some(tg::Selector::Id(parent))) => {
				params.extend(db::params![parent.to_string()]);
				conditions.push(format!(
					"coalesce(specifiers.parent, '') = {p}{}",
					params.len()
				));
			},
			Scope::Children(Some(tg::Selector::Specifier(parent))) => {
				params.extend(db::params![parent.to_string()]);
				conditions.push(format!(
					"coalesce(specifiers.parent, '') = (select id from specifiers where specifier = {p}{})",
					params.len()
				));
			},
			Scope::Descendants(specifier) => {
				let (start, end) = specifier.descendants_key_range();
				params.extend(db::params![bytes::Bytes::from(start)]);
				conditions.push(format!("specifiers.key >= {p}{}", params.len()));
				params.extend(db::params![bytes::Bytes::from(end)]);
				conditions.push(format!("specifiers.key < {p}{}", params.len()));
			},
		}
		if let Some(after) = after {
			let operator = if reverse { "<" } else { ">" };
			params.extend(db::params![bytes::Bytes::copy_from_slice(after)]);
			conditions.push(format!("specifiers.key {operator} {p}{}", params.len()));
		}
		let condition = if conditions.is_empty() {
			"true".to_owned()
		} else {
			conditions.join(" and ")
		};
		let direction = if reverse { "desc" } else { "asc" };
		params.extend(db::params![length.to_i64().unwrap_or(i64::MAX)]);
		let statement = format!(
			"
				select specifiers.id, specifiers.key, specifiers.parent, specifiers.specifier, tags.target
				from specifiers
				left join tags on tags.id = specifiers.id
				where {condition}
				order by specifiers.key {direction}
				limit {p}{};
			",
			params.len()
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
			items.push((row.key.to_vec(), id, item));
		}

		Ok(ControlFlow::Break(items))
	}

	async fn try_get_local_specifier(
		&self,
		session: &Session,
		id: &tg::Id,
	) -> tg::Result<Option<tg::Specifier>> {
		let id = id.clone();
		session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_local_specifier_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await
	}

	async fn try_get_local_specifier_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<ControlFlow<Option<tg::Specifier>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		let p = transaction.p();
		let statement = format!("select specifier from specifiers where id = {p}1;");
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let specifier = row.map(|row| row.specifier);

		Ok(ControlFlow::Break(specifier))
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
		// Read from the write connection, because these items decide what a delete removes.
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			..Default::default()
		};
		let items = session
			.server
			.database
			.run_with_options(options, |transaction| {
				async move { Self::list_local_items_with_transaction(transaction).await }.boxed()
			})
			.await?;

		// Filter the visible items.
		let items = self.filter_visible_items(session, items, tokens).await?;

		Ok(items)
	}

	async fn list_local_items_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Item)>, tangram_server_core::database::Error>>
	{
		let mut items = Vec::new();
		let groups = match Self::list_local_groups(transaction).await? {
			ControlFlow::Break(groups) => groups,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		items.extend(groups);
		let organizations = match Self::list_local_organizations(transaction).await? {
			ControlFlow::Break(organizations) => organizations,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		items.extend(organizations);
		let tags = match Self::list_local_tags(transaction).await? {
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
		let statement = "
			select groups.id, groups.parent, specifiers.specifier
			from groups
			join specifiers on specifiers.id = groups.id
			order by specifiers.specifier;
		";
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![])
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
		let statement = "
			select tags.id, tags.parent, specifiers.specifier, tags.target
			from tags
			join specifiers on specifiers.id = tags.id
			order by specifiers.specifier;
		";
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![])
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
	fn list_page<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: PageArg<'a>,
	) -> BoxFuture<'a, tg::Result<Page>> {
		self.list_page(session, arg).boxed()
	}

	fn list<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::list::Output>> {
		self.list(session, arg).boxed()
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

fn query_with_page(
	query: tangram_server_core::list::remote::Query,
	after: Option<Vec<u8>>,
	limit: usize,
) -> tg::Result<tangram_server_core::list::remote::Query> {
	let cursor = after
		.map(|after| Cursor::V0 { after })
		.as_ref()
		.map(tangram_server_core::cursor::serialize)
		.transpose()?;
	let limit = Some(u64::try_from(limit).unwrap());
	let query = match query {
		tangram_server_core::list::remote::Query::List(arg) => {
			tangram_server_core::list::remote::Query::List(tg::list::Arg {
				cursor,
				limit,
				..arg
			})
		},
		tangram_server_core::list::remote::Query::Match(arg) => {
			tangram_server_core::list::remote::Query::Match(tg::match_::Arg {
				cursor,
				limit,
				..arg
			})
		},
	};
	Ok(query)
}

/// Merge the pages, which are in order of precedence, into a page of at most `limit` items, keeping the first of the items that share a specifier.
fn merge_pages(pages: Vec<Page>, limit: usize, reverse: bool) -> Page {
	// Sort the items by key, keeping the order of the pages among the items with equal keys, and keep the first item with each specifier.
	let more = pages.iter().any(|page| page.next.is_some());
	let mut entries = pages
		.into_iter()
		.flat_map(|page| page.data)
		.map(|item| (item.specifier().key(), item))
		.collect::<Vec<_>>();
	if reverse {
		entries.sort_by(|a, b| b.0.cmp(&a.0));
	} else {
		entries.sort_by(|a, b| a.0.cmp(&b.0));
	}
	entries.dedup_by(|a, b| a.0 == b.0);

	// Select the page, which continues after its last item when any page has more items or items remain.
	let more = more || entries.len() > limit;
	entries.truncate(limit);
	let next = more
		.then(|| entries.last().map(|(key, _)| key.clone()))
		.flatten();
	let data = entries.into_iter().map(|(_, item)| item).collect();

	Page { data, next }
}
