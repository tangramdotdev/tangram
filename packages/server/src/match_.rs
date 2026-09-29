use {
	crate::{
		Session,
		cursor::{DEFAULT_LIMIT, MAX_LIMIT},
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { position: u64 },
}

impl Session {
	#[tracing::instrument(fields(pattern = %arg.pattern), level = "trace", name = "match", skip_all)]
	pub(crate) async fn match_(&self, mut arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		self.verify_request_with_network_access()?;

		// Parse the pagination arguments.
		let limit = arg.limit.unwrap_or(DEFAULT_LIMIT);
		if !(1..=MAX_LIMIT).contains(&limit) {
			return Err(tg::error!(
				"the page limit must be between 1 and {MAX_LIMIT}"
			));
		}
		let cursor = arg
			.cursor
			.as_deref()
			.map(crate::cursor::deserialize::<Cursor>)
			.transpose()?;
		let position = cursor.map_or(0, |cursor| match cursor {
			Cursor::V0 { position } => position,
		});

		// Collect the complete result before selecting the page.
		arg.cursor = None;
		arg.limit = None;
		let local_arg = arg.clone();
		let entries = self
			.query_specifier_entries(
				arg.location.as_ref(),
				&arg.tokens,
				arg.cached,
				arg.ttl,
				crate::list::remote::Query::Match(arg.clone()),
				move |entries| {
					let data = filter_entries(entries, &local_arg);
					crate::list::sort_entries(data, local_arg.reverse)
				},
			)
			.await?;
		let data = filter_entries(entries, &arg);
		let mut data = crate::list::sort_entries(data, arg.reverse);

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
			Some(crate::cursor::serialize(&cursor)?)
		} else {
			None
		};
		let data = data.drain(position..end).collect();

		let output = tg::match_::Output { cursor, data };

		Ok(output)
	}

	pub(crate) async fn match_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let output = self.match_(arg).await?;
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

fn filter_entries(
	entries: Vec<tg::match_::Entry>,
	arg: &tg::match_::Arg,
) -> Vec<tg::match_::Entry> {
	let kinds = crate::list::Kinds {
		groups: arg.groups,
		organizations: arg.organizations,
		tags: arg.tags,
		users: arg.users,
	};
	entries
		.into_iter()
		.filter(|entry| {
			arg.pattern.matches_specifier(entry.specifier())
				&& crate::list::entry_kind_enabled(entry, &kinds)
		})
		.collect()
}
