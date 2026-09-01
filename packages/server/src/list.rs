use {
	crate::Session,
	futures::{FutureExt as _, TryStreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

#[cfg(test)]
mod tests;

pub mod remote;

const DATABASE_PAGE_LENGTH: u64 = 256;

pub(crate) struct Kinds {
	pub groups: bool,
	pub organizations: bool,
	pub tags: bool,
	pub users: bool,
}

#[derive(Clone)]
enum Parent {
	Any,
	Id(tg::Id),
	Root,
}

impl Session {
	#[tracing::instrument(level = "trace", name = "list", skip_all)]
	pub(crate) async fn list(&self, mut arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.verify_request_with_network_access()?;
		if let Some(location) = arg
			.node
			.as_ref()
			.and_then(|node| node.options.location.clone())
		{
			arg.location = Some(location.into());
		}
		let local_only = arg
			.location
			.as_ref()
			.and_then(tg::location::Arg::to_location)
			.is_some_and(|location| match location {
				tg::Location::Local(local) => local
					.region
					.as_deref()
					.is_none_or(|region| Some(region) == self.server.config.region.as_deref()),
				tg::Location::Remote(_) => false,
			});
		if local_only && !arg.recursive {
			let data = self.list_local_entries_for_list(&arg).await?;

			return Ok(tg::list::Output { cursor: None, data });
		}
		let mut source_arg = arg.clone();
		source_arg.length = match (arg.position, arg.length) {
			(Some(position), Some(length)) => Some(position.saturating_add(length)),
			_ => arg.length,
		};
		source_arg.position = None;
		let local_arg = source_arg.clone();
		let tokens = arg
			.node
			.as_ref()
			.map(tg::Referent::options)
			.map(|options| options.tokens.clone())
			.unwrap_or_default();
		let entries = self
			.query_specifier_entries(
				arg.location.as_ref(),
				&tokens,
				arg.cached,
				arg.ttl,
				remote::Query::List(source_arg),
				move |entries| {
					let data = filter_list_entries(entries, &local_arg);
					sort_and_truncate(data, local_arg.reverse, None, local_arg.length)
				},
			)
			.await?;
		let data = sort_and_truncate(entries, arg.reverse, arg.position, arg.length);

		Ok(tg::list::Output { cursor: None, data })
	}

	pub(crate) async fn list_local_entries(&self) -> tg::Result<Vec<tg::list::Entry>> {
		self.list_local_entries_with_tokens(&tg::authorization::Tokens::default())
			.await
	}

	async fn list_local_entries_with_tokens(
		&self,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Vec<tg::list::Entry>> {
		// List the entries.
		let entries = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				async move { Self::list_local_entries_with_transaction(transaction).await }.boxed()
			})
			.await?;

		// Filter the visible entries.
		let entries = self.filter_visible_entries(entries, tokens).await?;

		Ok(entries)
	}

	pub(crate) async fn list_local_entries_for_list(
		&self,
		arg: &tg::list::Arg,
	) -> tg::Result<Vec<tg::list::Entry>> {
		if arg.length == Some(0) {
			return Ok(Vec::new());
		}

		// Resolve the parent.
		let parent = match &arg.node {
			None => Parent::Root,
			Some(node) => Parent::Id(node.node.clone()),
		};

		// Page through the database entries.
		let root = matches!(&self.context.principal, tg::Principal::Root);
		let mut database_position = if root {
			arg.position.unwrap_or_default()
		} else {
			0
		};
		let mut output = Vec::new();
		let mut output_position = if root {
			0
		} else {
			arg.position.unwrap_or_default()
		};
		let tokens = arg
			.node
			.as_ref()
			.map(tg::Referent::options)
			.map(|options| options.tokens.clone())
			.unwrap_or_default();
		loop {
			let database_length = if root {
				arg.length
					.map_or(DATABASE_PAGE_LENGTH, |length| {
						length.saturating_sub(output.len().to_u64().unwrap())
					})
					.clamp(1, DATABASE_PAGE_LENGTH)
			} else {
				DATABASE_PAGE_LENGTH
			};
			let arg = arg.clone();
			let parent = parent.clone();
			let entries = self
				.server
				.database
				.run_with_options(db::ConnectionOptions::default(), |transaction| {
					let arg = arg.clone();
					let parent = parent.clone();
					async move {
						Self::list_local_entries_for_list_with_transaction(
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
			let input_length = entries.len().to_u64().unwrap();
			let entries = self.filter_visible_entries(entries, &tokens).await?;
			for entry in entries {
				if output_position > 0 {
					output_position -= 1;
					continue;
				}
				output.push(entry);
				if arg
					.length
					.is_some_and(|length| output.len().to_u64().unwrap() >= length)
				{
					return Ok(output);
				}
			}
			if input_length < database_length {
				break;
			}
			database_position = database_position.saturating_add(input_length);
		}

		Ok(output)
	}

	async fn list_local_entries_with_transaction(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Entry)>, crate::database::Error>> {
		let mut entries = Vec::new();
		let groups = match Self::list_local_groups(transaction, &Parent::Any).await? {
			ControlFlow::Break(groups) => groups,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		entries.extend(groups);
		let organizations = match Self::list_local_organizations(transaction).await? {
			ControlFlow::Break(organizations) => organizations,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		entries.extend(organizations);
		let tags = match Self::list_local_tags(transaction, &Parent::Any).await? {
			ControlFlow::Break(tags) => tags,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		entries.extend(tags);
		let users = match Self::list_local_users(transaction).await? {
			ControlFlow::Break(users) => users,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		entries.extend(users);

		Ok(ControlFlow::Break(entries))
	}

	async fn list_local_entries_for_list_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::list::Arg,
		parent: &Parent,
		position: u64,
		length: u64,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Entry)>, crate::database::Error>> {
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
				from ({}) as entries
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
		let rows = crate::database::retry!(result, "failed to execute the statement");

		// Create the entries.
		let location = tg::Location::Local(tg::location::Local::default());
		let mut entries = Vec::with_capacity(rows.len());
		for row in rows {
			let id = row.id;
			let target = match id.kind() {
				tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User => None,
				tg::id::Kind::Tag => {
					let target = row
						.target
						.ok_or_else(|| tg::error!("expected a tag target"))?;
					let target = Self::parse_tag_target(&target)?;
					let target = Self::tag_target_referent(target, Some(location.clone()));
					Some(target)
				},
				_ => return Err(tg::error!(%id, "invalid list entry")),
			};
			let options = tg::referent::Options {
				location: Some(location.clone()),
				..Default::default()
			};
			let node = tg::Referent::new(id.clone(), options);
			let entry = tg::list::Entry {
				node,
				parent: row.parent,
				specifier: row.specifier,
				target,
			};
			entries.push((id, entry));
		}

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn query_specifier_entries<F>(
		&self,
		location: Option<&tg::location::Arg>,
		tokens: &tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
		query: remote::Query,
		filter_local: F,
	) -> tg::Result<Vec<tg::list::Entry>>
	where
		F: FnOnce(Vec<tg::list::Entry>) -> Vec<tg::list::Entry>,
	{
		let locations = self
			.locations(location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let mut sources = Vec::new();
		if locations.local.is_some() {
			let entries = self
				.list_local_entries_with_tokens(tokens)
				.await
				.map_err(|error| tg::error!(!error, "failed to list local entries"))?;
			let entries = filter_local(entries);
			sources.push(entries);
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
						.list_remote(remote.clone(), cached, ttl, query)
						.await
						.map_err(
							|error| tg::error!(!error, remote = %name, "failed to query remote entries"),
						)?;
					Ok::<_, tg::Error>((name, data))
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let mut remote_results = remote_results;
		remote_results.sort_by(|a, b| a.0.cmp(&b.0));
		sources.extend(remote_results.into_iter().map(|(_, entries)| entries));
		let entries = merge_entries(sources);

		Ok(entries)
	}

	async fn filter_visible_entries(
		&self,
		entries: Vec<(tg::Id, tg::list::Entry)>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Vec<tg::list::Entry>> {
		if entries.is_empty() {
			return Ok(Vec::new());
		}
		let ids = entries.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
		let visible = self
			.server
			.index
			.visible(&ids, &self.context.principal)
			.await?;
		let mut output = Vec::new();
		for ((id, mut entry), visible) in std::iter::zip(entries, visible) {
			let authorized = if visible {
				true
			} else {
				let permission = Self::read_permission_for_resource(&id)?;
				let resource = tg::Referent::with_node_and_tokens(
					tg::Selector::Id(id.clone()),
					tokens.clone(),
				);
				self.authorize(resource, permission)
					.await?
					.is_some_and(|permissions| permissions.contains(permission))
			};
			if authorized {
				let tokens = tg::authorization::Tokens::with_local(self.create_read_token(&id)?);
				if let Some(target) = &mut entry.target {
					target.options.tokens = tokens.clone();
				}
				entry.set_tokens(tokens);
				output.push(entry);
			}
		}

		Ok(output)
	}

	async fn list_local_groups(
		transaction: &crate::database::Transaction<'_>,
		parent: &Parent,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Entry)>, crate::database::Error>> {
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
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let entries = rows
			.into_iter()
			.map(|row| {
				let id: tg::Id = row.id.clone().into();
				let location = tg::Location::Local(tg::location::Local::default());
				let options = tg::referent::Options {
					location: Some(location),
					..Default::default()
				};
				let node = tg::Referent::new(id.clone(), options);
				let entry = tg::list::Entry {
					node,
					parent: row.parent,
					specifier: row.specifier,
					target: None,
				};
				(id, entry)
			})
			.collect();

		Ok(ControlFlow::Break(entries))
	}

	async fn list_local_organizations(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Entry)>, crate::database::Error>> {
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
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let entries = rows
			.into_iter()
			.map(|row| {
				let id: tg::Id = row.id.clone().into();
				let location = tg::Location::Local(tg::location::Local::default());
				let options = tg::referent::Options {
					location: Some(location),
					..Default::default()
				};
				let node = tg::Referent::new(id.clone(), options);
				let entry = tg::list::Entry {
					node,
					parent: None,
					specifier: row.specifier,
					target: None,
				};
				(id, entry)
			})
			.collect();

		Ok(ControlFlow::Break(entries))
	}

	async fn list_local_tags(
		transaction: &crate::database::Transaction<'_>,
		parent: &Parent,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Entry)>, crate::database::Error>> {
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
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let mut entries = Vec::new();
		for row in rows {
			let target = Self::parse_tag_target(&row.target)?;
			let location = tg::Location::Local(tg::location::Local::default());
			let id: tg::Id = row.id.clone().into();
			let options = tg::referent::Options {
				location: Some(location.clone()),
				..Default::default()
			};
			let node = tg::Referent::new(id.clone(), options);
			let target = Some(Self::tag_target_referent(target, Some(location)));
			let entry = tg::list::Entry {
				node,
				parent: row.parent,
				specifier: row.specifier,
				target,
			};
			entries.push((id, entry));
		}

		Ok(ControlFlow::Break(entries))
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
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::list::Entry)>, crate::database::Error>> {
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
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let entries = rows
			.into_iter()
			.map(|row| {
				let id: tg::Id = row.id.clone().into();
				let location = tg::Location::Local(tg::location::Local::default());
				let options = tg::referent::Options {
					location: Some(location),
					..Default::default()
				};
				let node = tg::Referent::new(id.clone(), options);
				let entry = tg::list::Entry {
					node,
					parent: None,
					specifier: row.specifier,
					target: None,
				};
				(id, entry)
			})
			.collect();

		Ok(ControlFlow::Break(entries))
	}

	pub(crate) async fn list_request(
		&self,
		request: http::Request<BoxBody>,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let mut arg: tg::list::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		if !path.is_empty() {
			let id = path
				.join("/")
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the list node"))?;
			let options = request
				.query_params::<tg::referent::Options>()
				.transpose()
				.map_err(|error| tg::error!(!error, "failed to parse the referent options"))?
				.unwrap_or_default();
			arg.node = Some(tg::Referent::new(id, options));
		}
		let output = self.list(arg).await?;
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

pub(crate) fn entry_kind_enabled(entry: &tg::list::Entry, kinds: &Kinds) -> bool {
	match entry.kind() {
		tg::id::Kind::Group => kinds.groups,
		tg::id::Kind::Organization => kinds.organizations,
		tg::id::Kind::Tag => kinds.tags,
		tg::id::Kind::User => kinds.users,
		_ => false,
	}
}

pub(crate) fn sort_and_truncate(
	mut data: Vec<tg::list::Entry>,
	reverse: bool,
	position: Option<u64>,
	length: Option<u64>,
) -> Vec<tg::list::Entry> {
	data.sort_by(|a, b| compare_entries(a, b, reverse));
	let position = position
		.map(|position| position.to_usize().unwrap_or(usize::MAX))
		.unwrap_or_default()
		.min(data.len());
	data.drain(..position);
	if let Some(length) = length {
		data.truncate(length.to_usize().unwrap());
	}

	data
}

fn filter_list_entries(entries: Vec<tg::list::Entry>, arg: &tg::list::Arg) -> Vec<tg::list::Entry> {
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
			for entry in &entries {
				let include = match (parent.as_ref(), entry.parent()) {
					(None, _) => true,
					(Some(_), Some(parent)) => descendants.contains(parent),
					(Some(_), None) => false,
				};
				if include {
					descendants.insert(entry.id().clone());
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
	entries
		.into_iter()
		.filter(|entry| {
			let matches_parent = if let Some(descendants) = &descendants {
				(parent.is_none() || descendants.contains(entry.id()))
					&& parent.as_ref() != Some(entry.id())
			} else {
				entry.parent() == parent.as_ref()
			};
			matches_parent && entry_kind_enabled(entry, &kinds)
		})
		.collect()
}

fn merge_entries(sources: Vec<Vec<tg::list::Entry>>) -> Vec<tg::list::Entry> {
	let mut emitted = BTreeSet::new();
	let mut output = Vec::new();
	for entries in sources {
		output.extend(
			entries
				.into_iter()
				.filter(|entry| emitted.insert(entry.specifier().clone())),
		);
	}

	output
}

fn compare_entries(a: &tg::list::Entry, b: &tg::list::Entry, reverse: bool) -> std::cmp::Ordering {
	let order = tg::list::compare(&a.specifier().to_string(), &b.specifier().to_string());
	let order = if reverse { order.reverse() } else { order };
	order.then_with(|| entry_kind(a).cmp(&entry_kind(b)))
}

fn entry_kind(entry: &tg::list::Entry) -> tg::id::Kind {
	entry.kind()
}
