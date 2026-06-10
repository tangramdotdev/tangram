use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) async fn list_group_members(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> tg::Result<tg::group::members::list::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.list_group_members_local(group).await,
			tg::Location::Remote(remote) => {
				self.list_group_members_remote(group, arg, remote).await
			},
		}
	}

	async fn list_group_members_local(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<tg::group::members::list::Output> {
		let group = self
			.try_get_node_by_selector(group)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		if group.kind != tg::id::Kind::Group {
			return Err(tg::error!("failed to find the group"));
		}
		if !self
			.authorize(group.id.clone(), tg::grant::Permission::Read)
			.await?
		{
			return Err(tg::error!("failed to find the group"));
		}
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
			member: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select member
				from group_members
				where "group" = {p}1
				order by member;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![group.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| row.member.try_into())
			.collect::<tg::Result<_>>()?;
		Ok(tg::group::members::list::Output { data })
	}

	async fn list_group_members_remote(
		&self,
		group: &tg::group::Selector,
		mut arg: tg::group::members::list::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::group::members::list::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.list_group_members(group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to list the group members"),
		)
	}

	pub(crate) async fn list_group_members_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
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
		let group = group.replace(':', "/").parse()?;
		let output = self.list_group_members(&group, arg).await?;
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
