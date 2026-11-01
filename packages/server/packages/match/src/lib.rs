use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { position: u64 },
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[tracing::instrument(fields(pattern = %arg.pattern), level = "trace", name = "match", skip_all)]
	pub(crate) async fn match_(
		&self,
		session: &Session,
		mut arg: tg::match_::Arg,
	) -> tg::Result<tg::match_::Output> {
		session.verify_request_with_network_access()?;

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
		let local_arg = arg.clone();
		let query_arg: tangram_server_core::list::QuerySpecifierEntriesArg =
			tangram_server_core::list::QuerySpecifierEntriesArg {
				cached: arg.cached,
				filter_local: Box::new(move |entries| {
					let data = filter_entries(entries, &local_arg);
					tangram_server_list_common::sort_entries(data, local_arg.reverse)
				}),
				location: arg.location.as_ref(),
				query: tangram_server_core::list::remote::Query::Match(arg.clone()),
				tokens: &arg.tokens,
				ttl: arg.ttl,
			};
		let entries = session
			.server
			.components
			.list
			.query_specifier_entries(session, query_arg)
			.await?;
		let data = filter_entries(entries, &arg);
		let mut data = tangram_server_list_common::sort_entries(data, arg.reverse);

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

		let output = tg::match_::Output { cursor, data };

		Ok(output)
	}

	pub(crate) async fn match_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let output = self.match_(session, arg).await?;
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
			(http::Method::GET, ["match"]) => self.match_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::match_::Component for Component {
	fn match_<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::match_::Arg,
	) -> BoxFuture<'a, tg::Result<tg::match_::Output>> {
		self.match_(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}

fn filter_entries(entries: Vec<tg::match_::Item>, arg: &tg::match_::Arg) -> Vec<tg::match_::Item> {
	let kinds = tangram_server_list_common::Kinds {
		groups: arg.groups,
		organizations: arg.organizations,
		tags: arg.tags,
		users: arg.users,
	};
	entries
		.into_iter()
		.filter(|entry| {
			arg.pattern.matches_specifier(entry.specifier())
				&& tangram_server_list_common::entry_kind_enabled(entry, &kinds)
		})
		.collect()
}
