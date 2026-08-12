use {
	crate::Session,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

pub mod remote;

pub(crate) struct Kinds {
	pub groups: bool,
	pub organizations: bool,
	pub tags: bool,
	pub users: bool,
}

impl Session {
	#[tracing::instrument(level = "trace", name = "list", skip_all)]
	pub(crate) async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.verify_request_with_network_access()?;
		let local_arg = arg.clone();
		let entries = self
			.query_specifier_entries(
				arg.location.as_ref(),
				arg.cached,
				arg.ttl,
				remote::Query::List(arg.clone()),
				move |entries| {
					let data = filter_list_entries(entries, &local_arg);
					sort_and_truncate(data, local_arg.reverse, local_arg.length)
				},
			)
			.await?;
		let data = sort_and_truncate(entries, arg.reverse, arg.length);

		Ok(tg::list::Output { data })
	}

	pub(crate) async fn list_local_entries(&self) -> tg::Result<Vec<tg::list::Entry>> {
		// List the entries.
		let entries = {
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
			let mut entries = Vec::new();
			entries.extend(Self::list_local_groups(&transaction).await?);
			entries.extend(Self::list_local_organizations(&transaction).await?);
			entries.extend(Self::list_local_tags(&transaction).await?);
			entries.extend(Self::list_local_users(&transaction).await?);
			entries
		};

		// Filter the visible entries.
		let entries = self.filter_visible_entries(entries).await?;

		Ok(entries)
	}

	pub(crate) async fn query_specifier_entries<F>(
		&self,
		location: Option<&tg::location::Arg>,
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
				.list_local_entries()
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
				self.authorize(tg::Selector::Id(id.clone()), permission)
					.await?
					.is_some_and(|permissions| permissions.contains(permission))
			};
			if authorized {
				let tokens = tg::authorization::Tokens::with_local(self.create_read_token(&id)?);
				entry.set_tokens(tokens);
				output.push(entry);
			}
		}

		Ok(output)
	}

	async fn list_local_groups(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<Vec<(tg::Id, tg::list::Entry)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::group::Id,
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select groups.id, groups.name, groups.parent, specifiers.specifier
					from groups
					join specifiers on specifiers.id = groups.id
					order by specifiers.specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let entries = rows
			.into_iter()
			.map(|row| {
				let id = row.id.clone().into();
				let entry = tg::list::Entry::Group {
					id: row.id,
					location: Some(tg::Location::Local(tg::location::Local::default())),
					name: row.name,
					parent: row.parent,
					specifier: row.specifier,
					tokens: tg::authorization::Tokens::default(),
				};
				(id, entry)
			})
			.collect();

		Ok(entries)
	}

	async fn list_local_organizations(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<Vec<(tg::Id, tg::list::Entry)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::organization::Id,
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select organizations.id, organizations.name, specifiers.specifier
					from organizations
					join specifiers on specifiers.id = organizations.id
					order by specifiers.specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let entries = rows
			.into_iter()
			.map(|row| {
				let id = row.id.clone().into();
				let entry = tg::list::Entry::Organization {
					id: row.id,
					location: Some(tg::Location::Local(tg::location::Local::default())),
					name: row.name,
					specifier: row.specifier,
					tokens: tg::authorization::Tokens::default(),
				};
				(id, entry)
			})
			.collect();

		Ok(entries)
	}

	async fn list_local_tags(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<Vec<(tg::Id, tg::list::Entry)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::tag::Id,
			target: String,
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select tags.id, tags.target, tags.name, tags.parent, specifiers.specifier
					from tags
					join specifiers on specifiers.id = tags.id
					order by specifiers.specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut entries = Vec::new();
		for row in rows {
			let target = Self::parse_tag_target(&row.target)?;
			let target = match target {
				tg::tag::data::Target::Object(id) => tg::Either::Left(id),
				tg::tag::data::Target::Process(id) => tg::Either::Right(id),
			};
			let id = row.id.clone().into();
			let entry = tg::list::Entry::Tag {
				id: row.id,
				target,
				location: Some(tg::Location::Local(tg::location::Local::default())),
				name: row.name,
				parent: row.parent,
				specifier: row.specifier,
				tokens: tg::authorization::Tokens::default(),
			};
			entries.push((id, entry));
		}

		Ok(entries)
	}

	async fn list_local_users(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<Vec<(tg::Id, tg::list::Entry)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select users.id, users.name, specifiers.specifier
					from users
					join specifiers on specifiers.id = users.id
					order by specifiers.specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let entries = rows
			.into_iter()
			.map(|row| {
				let id = row.id.clone().into();
				let entry = tg::list::Entry::User {
					id: row.id,
					location: Some(tg::Location::Local(tg::location::Local::default())),
					name: row.name,
					specifier: row.specifier,
					tokens: tg::authorization::Tokens::default(),
				};
				(id, entry)
			})
			.collect();

		Ok(entries)
	}

	pub(crate) async fn list_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
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
	match entry {
		tg::list::Entry::Group { .. } => kinds.groups,
		tg::list::Entry::Organization { .. } => kinds.organizations,
		tg::list::Entry::Tag { .. } => kinds.tags,
		tg::list::Entry::User { .. } => kinds.users,
	}
}

pub(crate) fn sort_and_truncate(
	mut data: Vec<tg::list::Entry>,
	reverse: bool,
	length: Option<u64>,
) -> Vec<tg::list::Entry> {
	data.sort_by(|a, b| compare_entries(a, b, reverse));
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
	let parent = arg.parent.as_ref().and_then(|parent| match parent {
		tg::Selector::Id(id) => Some(id.clone()),
		tg::Selector::Specifier(specifier) => entries
			.iter()
			.find(|entry| entry.specifier() == specifier)
			.map(tg::list::Entry::id),
	});
	if arg.parent.is_some() && parent.is_none() {
		return Vec::new();
	}
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
					descendants.insert(entry.id());
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
				(parent.is_none() || descendants.contains(&entry.id()))
					&& parent.as_ref() != Some(&entry.id())
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
	match entry {
		tg::list::Entry::Group { .. } => tg::id::Kind::Group,
		tg::list::Entry::Organization { .. } => tg::id::Kind::Organization,
		tg::list::Entry::Tag { .. } => tg::id::Kind::Tag,
		tg::list::Entry::User { .. } => tg::id::Kind::User,
	}
}
