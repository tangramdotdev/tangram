use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Server {
	pub(crate) async fn list_sandboxes_with_context(
		&self,
		context: &Context,
		_arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::sandbox::Id,
			hostname: Option<String>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::sandbox::Mount>>>")]
			mounts: Option<Vec<tg::sandbox::Mount>>,
			network: bool,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::sandbox::Status,
			ttl: Option<i64>,
			user: Option<String>,
		}
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = formatdoc!(
			"
				select id, hostname, mounts, network, status, ttl, \"user\" as user
				from sandboxes
				where status != 'finished'
				order by created_at;
			"
		);
		let params = db::params![];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| tg::sandbox::list::Item {
				id: row.id,
				hostname: row.hostname,
				mounts: row.mounts.unwrap_or_default(),
				network: row.network,
				status: row.status,
				ttl: row.ttl.map(|ttl| u64::try_from(ttl).unwrap()),
				user: row.user,
			})
			.collect();
		Ok(tg::sandbox::list::Output { data })
	}

	pub(crate) async fn handle_list_sandboxes_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		let output = self
			.list_sandboxes_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to list the sandboxes"))?;

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
