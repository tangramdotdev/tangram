use {
	crate::{
		Session,
		cursor::{DEFAULT_LIMIT, MAX_LIMIT},
	},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { after: tg::Id },
}

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
			tg::Location::Local(_) => self.list_group_members_local(group, arg).await,
			tg::Location::Remote(remote) => {
				self.list_group_members_remote(group, arg, remote).await
			},
		}
	}

	async fn list_group_members_local(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> tg::Result<tg::group::members::list::Output> {
		// Authorize the group.
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		);
		let authorized = self
			.authorize(group.clone(), permission)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Err(tg::error!("failed to find the group"));
		}

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
		let after = cursor.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});

		// List the members.
		let group = group.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.clone();
				let group = group.clone();
				async move {
					Self::list_group_members_local_with_transaction(
						transaction,
						&group,
						after.as_ref(),
						limit,
					)
					.await
				}
				.boxed()
			})
			.await
	}

	async fn list_group_members_local_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		after: Option<&tg::Id>,
		limit: u64,
	) -> tg::Result<ControlFlow<tg::group::members::list::Output, crate::database::Error>> {
		let id = match group {
			tg::Selector::Id(id) => Some(id.clone()),
			tg::Selector::Specifier(specifier) => {
				let id =
					match Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
						.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				id.and_then(|id| id.try_into().ok())
			},
		}
		.ok_or_else(|| tg::error!("failed to find the group"))?;
		let group = match Self::try_get_group_with_transaction(transaction, &id).await? {
			ControlFlow::Break(group) => group,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if group.is_none() {
			return Err(tg::error!("failed to find the group"));
		}
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
				and member > {p}2
				order by member
				limit {p}3;
			"#
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![
					id.to_string(),
					after.map(ToString::to_string).unwrap_or_default(),
					i64::try_from(limit + 1).unwrap()
				],
			)
			.await;
		let mut rows = crate::database::retry!(result, "failed to execute the statement");

		// Create the continuation cursor.
		let cursor = if u64::try_from(rows.len()).unwrap() > limit {
			rows.pop();
			let cursor = Cursor::V0 {
				after: rows.last().unwrap().member.clone(),
			};
			Some(crate::cursor::serialize(&cursor)?)
		} else {
			None
		};

		// Create the output.
		let data = rows
			.into_iter()
			.map(|row| row.member.try_into())
			.collect::<tg::Result<_>>()?;

		let output = tg::group::members::list::Output { cursor, data };

		Ok(ControlFlow::Break(output))
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
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let group = group
			.replace(':', "/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the group"))?;
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
