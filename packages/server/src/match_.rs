use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	#[tracing::instrument(fields(pattern = %arg.pattern), level = "trace", name = "match", skip_all)]
	pub(crate) async fn match_(&self, arg: tg::match_::Arg) -> tg::Result<tg::match_::Output> {
		self.verify_request_with_network_access()?;
		let local_arg = arg.clone();
		let entries = self
			.query_specifier_entries(
				arg.location.as_ref(),
				arg.cached,
				arg.ttl,
				crate::list::remote::Query::Match(arg.clone()),
				move |entries| {
					let data = filter_entries(entries, &local_arg);
					crate::list::sort_and_truncate(data, local_arg.reverse, None, local_arg.length)
				},
			)
			.await?;
		let data = filter_entries(entries, &arg);
		let data = crate::list::sort_and_truncate(data, arg.reverse, None, arg.length);

		Ok(tg::match_::Output { data })
	}

	pub(crate) async fn match_request(
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
