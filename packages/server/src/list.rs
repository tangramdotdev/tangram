use {
	crate::{Database, Session},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	time::OffsetDateTime,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub(crate) struct RemoteListTaskKey {
	pub remote: String,
	pub arg: tg::list::Arg,
}

impl Session {
	#[tracing::instrument(fields(pattern = %arg.pattern), level = "trace", name = "list", skip_all)]
	pub(crate) async fn list(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		if !arg.pattern.is_empty() && !arg.pattern.contains_operators() {
			if arg.tags
				&& let Some(pattern) = arg.pattern.exact()
			{
				let output = self
					.list_inner(tg::list::Arg {
						length: None,
						namespaces: false,
						pattern,
						recursive: false,
						..arg.clone()
					})
					.await?;
				if !output.data.is_empty() {
					return Ok(truncate(output, arg.length));
				}
			}
			if let Some(pattern) = arg.pattern.children() {
				return self.list_inner(tg::list::Arg { pattern, ..arg }).await;
			}
		}

		self.list_inner(arg).await
	}

	async fn list_inner(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		self.authorize_list()
			.map_err(|error| tg::error!(!error, "failed to authorize"))?;

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
			data.extend(
				self.filter_list_entries_by_read_permission(output.data)
					.await?,
			);
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

	pub(crate) async fn list_local(&self, arg: tg::list::Arg) -> tg::Result<tg::list::Output> {
		match &self.server.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_postgres(database, arg).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.list_sqlite(database, arg).await,
		}
	}

	async fn list_remote(
		&self,
		remote: crate::location::Remote,
		arg: &tg::list::Arg,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let remote_arg = remote_arg(arg, remote.regions.clone());
		let key = RemoteListTaskKey {
			remote: remote.name.clone(),
			arg: remote_arg.clone(),
		};
		let key_json = serde_json::to_string(&key).unwrap();

		let use_cache = self.server.config().authentication.is_none();
		if use_cache
			&& arg.ttl != Some(Duration::ZERO)
			&& let Some((cached_output, timestamp)) = self
				.list_cache_get(&key_json)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the list cache"))?
		{
			let now = OffsetDateTime::now_utc().unix_timestamp();
			let age = u64::try_from((now - timestamp).max(0))
				.map(Duration::from_secs)
				.map_err(|error| tg::error!(!error, "invalid list cache age"))?;
			if arg.ttl.is_none_or(|ttl| age < ttl) {
				let mut entries: Vec<tg::list::Entry> = serde_json::from_str(&cached_output)
					.map_err(|error| tg::error!(!error, "failed to deserialize the cached list"))?;
				for entry in &mut entries {
					set_entry_location(entry, &remote.name);
				}
				return Ok(entries);
			}
		}

		if arg.cached {
			return Ok(Vec::new());
		}

		let task = self
			.server
			.remote_list_tasks
			.get_or_spawn_detached(key.clone(), {
				let session = self.clone();
				move |_stop| async move { session.list_remote_task(key).await }
			});
		let entries = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the remote list task panicked"))??;
		let entries = entries
			.into_iter()
			.map(|mut entry| {
				set_entry_location(&mut entry, &remote.name);
				entry
			})
			.collect();

		Ok(entries)
	}

	async fn list_remote_task(&self, key: RemoteListTaskKey) -> tg::Result<Vec<tg::list::Entry>> {
		let RemoteListTaskKey { remote, arg } = key;
		let client = self
			.get_remote_session(remote.clone())
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let output = client
			.list(arg.clone())
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to list entries"))?;

		if self.server.config().authentication.is_none() {
			let key = serde_json::to_string(&RemoteListTaskKey { remote, arg }).unwrap();
			let output_json = serde_json::to_string(&output.data).unwrap();
			let now = OffsetDateTime::now_utc().unix_timestamp();
			self.list_cache_put(&key, &output_json, now)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the list cache"))?;
		}

		Ok(output.data)
	}

	async fn list_cache_get(&self, arg: &str) -> tg::Result<Option<(String, i64)>> {
		match &self.server.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_cache_get_postgres(database, arg).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.list_cache_get_sqlite(database, arg).await,
		}
	}

	async fn list_cache_put(&self, arg: &str, output: &str, timestamp: i64) -> tg::Result<()> {
		match &self.server.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.list_cache_put_postgres(database, arg, output, timestamp)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				self.list_cache_put_sqlite(database, arg, output, timestamp)
					.await
			},
		}
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

fn remote_arg(arg: &tg::list::Arg, regions: Option<Vec<String>>) -> tg::list::Arg {
	tg::list::Arg {
		cached: false,
		length: None,
		location: Some(tg::location::Arg(vec![
			tg::location::arg::Component::Local(tg::location::arg::LocalComponent { regions }),
		])),
		reverse: false,
		ttl: None,
		..arg.clone()
	}
}

fn set_entry_location(entry: &mut tg::list::Entry, remote: &str) {
	let location = match entry {
		tg::list::Entry::Namespace { location, .. } | tg::list::Entry::Tag { location, .. } => {
			location.take()
		},
	};
	let region = match location {
		Some(tg::Location::Local(local)) => local.region,
		_ => None,
	};
	let location = Some(tg::Location::Remote(tg::location::Remote {
		name: remote.to_owned(),
		region,
	}));
	match entry {
		tg::list::Entry::Namespace {
			location: entry_location,
			..
		}
		| tg::list::Entry::Tag {
			location: entry_location,
			..
		} => *entry_location = location,
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
		(
			tg::list::Entry::Namespace { namespace: a, .. },
			tg::list::Entry::Namespace { namespace: b, .. },
		) => a.cmp(b),
		(tg::list::Entry::Namespace { namespace: a, .. }, tg::list::Entry::Tag { tag: b, .. }) => {
			a.to_string().cmp(&b.to_string())
		},
		(tg::list::Entry::Tag { tag: a, .. }, tg::list::Entry::Namespace { namespace: b, .. }) => {
			a.to_string().cmp(&b.to_string())
		},
		(tg::list::Entry::Tag { tag: a, .. }, tg::list::Entry::Tag { tag: b, .. }) => a.cmp(b),
	}
}

fn compare_entry_kinds(a: &tg::list::Entry, b: &tg::list::Entry) -> std::cmp::Ordering {
	match (a, b) {
		(tg::list::Entry::Namespace { .. }, tg::list::Entry::Tag { .. }) => {
			std::cmp::Ordering::Less
		},
		(tg::list::Entry::Tag { .. }, tg::list::Entry::Namespace { .. }) => {
			std::cmp::Ordering::Greater
		},
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
		tg::list::Entry::Namespace { location, .. } | tg::list::Entry::Tag { location, .. } => {
			location.as_ref()
		},
	}
}
