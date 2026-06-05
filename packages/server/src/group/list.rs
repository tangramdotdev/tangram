use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) async fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> tg::Result<tg::group::list::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.list_groups_local().await,
			tg::Location::Remote(remote) => self.list_groups_remote(arg, remote).await,
		}
	}

	async fn list_groups_local(&self) -> tg::Result<tg::group::list::Output> {
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
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let rows = transaction
			.query_all_into::<Row>(
				"
					select nodes.id, nodes.name, nodes.parent, nodes.specifier
					from nodes
					where kind = 'group'
					order by specifier;
				"
				.into(),
				db::params![],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut data = Vec::new();
		for row in rows {
			if self
				.node_is_visible_with_transaction(&transaction, &row.id)
				.await?
			{
				data.push(tg::Group {
					id: row.id.try_into()?,
					name: row.name,
					parent: row.parent,
					specifier: row.specifier,
				});
			}
		}
		Ok(tg::group::list::Output { data })
	}

	async fn list_groups_remote(
		&self,
		mut arg: tg::group::list::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::group::list::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.list_groups(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to list the groups"))
	}

	pub(crate) async fn list_groups_request(
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
		let output = self.list_groups(arg).await?;
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
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}
}
