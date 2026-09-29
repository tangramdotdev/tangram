use {
	crate::{
		Session,
		cursor::{DEFAULT_LIMIT, MAX_LIMIT},
	},
	std::path::PathBuf,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { after: PathBuf },
}

impl Session {
	pub(crate) async fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> tg::Result<tg::watch::list::Output> {
		self.verify_request_from_host()?;

		// Parse the arg.
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
		let after = cursor.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});

		// Sort the paths for the current principal.
		let mut paths = self
			.server
			.watches
			.iter()
			.filter_map(|entry| {
				(entry.key().principal == self.context.principal).then(|| entry.key().path.clone())
			})
			.collect::<Vec<_>>();
		paths.sort();
		let mut paths = paths
			.into_iter()
			.filter(|path| after.as_ref().is_none_or(|after| path > after))
			.take(usize::try_from(limit + 1).unwrap())
			.collect::<Vec<_>>();

		// Create the continuation cursor.
		let cursor = if u64::try_from(paths.len()).unwrap() > limit {
			paths.pop();
			let cursor = Cursor::V0 {
				after: paths.last().unwrap().clone(),
			};
			Some(crate::cursor::serialize(&cursor)?)
		} else {
			None
		};

		// Create the output.
		let data = paths
			.into_iter()
			.map(|path| tg::watch::list::Item { path })
			.collect();
		let output = tg::watch::list::Output { cursor, data };

		Ok(output)
	}

	pub(crate) async fn list_watches_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// List the watches.
		let output = self
			.list_watches(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the watches"))?;

		// Create the response.
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
}
