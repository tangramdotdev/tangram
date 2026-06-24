use {
	crate::Session,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

pub mod remote;

impl Session {
	#[tracing::instrument(fields(pattern = %arg.pattern), level = "trace", name = "list", skip_all)]
	pub(crate) async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}

		self.list_inner(arg).await
	}

	async fn list_inner(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		let mut data = Vec::new();
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if locations.local.is_some() {
			let output = self
				.list_local(tg::list::Arg {
					length: None,
					..arg.clone()
				})
				.await
				.map_err(|error| tg::error!(!error, "failed to list local entries"))?;
			data.extend(output.data);
		}

		let remote_results = locations
			.remotes
			.into_iter()
			.map(|remote| {
				let arg = arg.clone();
				async move { self.list_remote(remote, &arg).await }
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		data.extend(remote_results.into_iter().flatten());

		data.sort_by(|a, b| compare_entries(a, b, arg.reverse));

		Ok(truncate(tg::list::Output { data }, arg.length))
	}

	pub(super) async fn list_cache_get(&self, arg: &str) -> tg::Result<Option<(String, i64)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			output: String,
			timestamp: i64,
		}
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select output, timestamp
				from list_cache
				where arg = {p}1;
			"
		);
		let row = connection
			.query_optional_into::<Row>(statement.into(), db::params![arg])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(row.map(|row| (row.output, row.timestamp)))
	}

	pub(super) async fn list_cache_put(
		&self,
		arg: &str,
		output: &str,
		timestamp: i64,
	) -> tg::Result<()> {
		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into list_cache (arg, output, timestamp)
				values ({p}1, {p}2, {p}3)
				on conflict (arg) do update
				set output = excluded.output, timestamp = excluded.timestamp;
			"
		);
		connection
			.execute(statement.into(), db::params![arg, output, timestamp])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(())
	}

	pub(crate) async fn list_local(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
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
		if arg.groups {
			entries.extend(Self::list_local_groups(&transaction, &arg).await?);
		}
		if arg.tags {
			entries.extend(Self::list_local_tags(&transaction, &arg).await?);
		}
		let mut data = self.filter_visible_entries(&arg, entries).await?;
		data.sort_by(|a, b| compare_entries(a, b, arg.reverse));
		Ok(truncate(tg::list::Output { data }, arg.length))
	}

	async fn filter_visible_entries(
		&self,
		arg: &tg::list::Arg,
		entries: Vec<(tg::Id, tg::list::Entry)>,
	) -> tg::Result<Vec<tg::list::Entry>> {
		if entries.is_empty() {
			return Ok(Vec::new());
		}
		if !arg.pattern.is_empty() && !arg.pattern.contains_operators() {
			let resource = tg::grant::Resource::Specifier(arg.pattern.to_specifier());
			if let Ok(id) = self.resolve_resource(&resource).await {
				let permission = Self::read_permission_for_resource(&id)?;
				let authorized = self.authorize(resource, permission).await?;
				if authorized.is_some_and(|permissions| permissions.contains(permission)) {
					return Ok(entries.into_iter().map(|(_, entry)| entry).collect());
				}
			}
		}
		let ids = entries.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
		let visible = self
			.server
			.index
			.visible(&ids, &self.context.principal)
			.await?;
		let entries = entries
			.into_iter()
			.zip(visible)
			.filter_map(|((_, entry), visible)| visible.then_some(entry))
			.collect();
		Ok(entries)
	}

	async fn list_local_groups(
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::list::Arg,
	) -> tg::Result<Vec<(tg::Id, tg::list::Entry)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select id, specifier
					from nodes
					where kind = 'group'
					order by specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut entries = Vec::new();
		for row in rows {
			if !matches_pattern(&row.specifier, arg) {
				continue;
			}
			let entry = tg::list::Entry::Group {
				location: None,
				group: row.specifier,
			};
			entries.push((row.id, entry));
		}
		Ok(entries)
	}

	async fn list_local_tags(
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::list::Arg,
	) -> tg::Result<Vec<(tg::Id, tg::list::Entry)>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			item: String,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select nodes.id, tags.item, nodes.specifier
				from nodes
				join tags on tags.id = nodes.id
				where nodes.kind = 'tag'
				order by nodes.specifier
				limit {p}1;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![
					arg.length
						.and_then(|length| length.to_i64())
						.unwrap_or(i64::MAX)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut entries = Vec::new();
		for row in rows {
			if !matches_pattern(&row.specifier, arg) {
				continue;
			}
			let item = Self::parse_tag_item(&row.item)?;
			let item = match item {
				tg::tag::data::Item::Object(id) => tg::Either::Left(id),
				tg::tag::data::Item::Process(id) => tg::Either::Right(id),
			};
			let entry = tg::list::Entry::Tag {
				item,
				location: None,
				tag: row.specifier,
			};
			entries.push((row.id, entry));
		}
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

fn truncate(mut output: tg::list::Output, length: Option<u64>) -> tg::list::Output {
	if let Some(length) = length {
		output.data.truncate(length.to_usize().unwrap());
	}
	output
}

fn compare_entries(a: &tg::list::Entry, b: &tg::list::Entry, reverse: bool) -> std::cmp::Ordering {
	let order = compare_entry_names(a, b);
	let order = if reverse { order.reverse() } else { order };
	order
		.then_with(|| compare_entry_kinds(a, b))
		.then_with(|| compare_entry_locations(a, b))
}

fn compare_entry_names(a: &tg::list::Entry, b: &tg::list::Entry) -> std::cmp::Ordering {
	match (a, b) {
		(tg::list::Entry::Group { group: a, .. }, tg::list::Entry::Group { group: b, .. }) => {
			a.cmp(b)
		},
		(tg::list::Entry::Group { group: a, .. }, tg::list::Entry::Tag { tag: b, .. })
		| (tg::list::Entry::Tag { tag: a, .. }, tg::list::Entry::Group { group: b, .. }) => {
			a.to_string().cmp(&b.to_string())
		},
		(tg::list::Entry::Tag { tag: a, .. }, tg::list::Entry::Tag { tag: b, .. }) => a.cmp(b),
	}
}

fn compare_entry_kinds(a: &tg::list::Entry, b: &tg::list::Entry) -> std::cmp::Ordering {
	match (a, b) {
		(tg::list::Entry::Group { .. }, tg::list::Entry::Tag { .. }) => std::cmp::Ordering::Less,
		(tg::list::Entry::Tag { .. }, tg::list::Entry::Group { .. }) => std::cmp::Ordering::Greater,
		_ => std::cmp::Ordering::Equal,
	}
}

fn compare_entry_locations(a: &tg::list::Entry, b: &tg::list::Entry) -> std::cmp::Ordering {
	let a = entry_location(a);
	let b = entry_location(b);
	match (a, b) {
		(None, None) | (Some(tg::Location::Local(_)), Some(tg::Location::Local(_))) => {
			std::cmp::Ordering::Equal
		},
		(None, Some(_)) | (Some(tg::Location::Local(_)), Some(tg::Location::Remote(_))) => {
			std::cmp::Ordering::Less
		},
		(Some(_), None) => std::cmp::Ordering::Greater,
		(Some(tg::Location::Remote(a)), Some(tg::Location::Remote(b))) => a.cmp(b),
		(Some(tg::Location::Remote(_)), Some(tg::Location::Local(_))) => {
			std::cmp::Ordering::Greater
		},
	}
}

fn entry_location(entry: &tg::list::Entry) -> Option<&tg::Location> {
	match entry {
		tg::list::Entry::Group { location, .. } | tg::list::Entry::Tag { location, .. } => {
			location.as_ref()
		},
	}
}

fn matches_pattern(specifier: &tg::Specifier, arg: &tg::list::Arg) -> bool {
	if arg.pattern.is_empty() {
		return true;
	}
	if !arg.recursive {
		return arg.pattern.matches_specifier_for_list(specifier);
	}
	pattern_matches_specifier_or_ancestor(&arg.pattern, specifier)
		|| arg
			.pattern
			.children()
			.is_some_and(|pattern| pattern_matches_specifier_or_ancestor(&pattern, specifier))
}

fn pattern_matches_specifier_or_ancestor(
	pattern: &tg::specifier::Pattern,
	specifier: &tg::Specifier,
) -> bool {
	for ancestor in specifier.prefixes() {
		if pattern.matches_specifier(&ancestor) {
			return true;
		}
	}
	false
}
