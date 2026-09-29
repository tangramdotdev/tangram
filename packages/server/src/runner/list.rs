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
	V0 { after: tg::runner::Id },
}

#[derive(db::row::Deserialize)]
struct Row {
	created_at: i64,

	#[tangram_database(as = "db::value::FromStr")]
	id: tg::runner::Id,

	#[tangram_database(as = "Option<db::value::FromStr>")]
	owner: Option<tg::Id>,
}

impl Session {
	pub(crate) async fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> tg::Result<tg::runner::list::Output> {
		self.verify_request_from_host()?;

		// Resolve and authorize the owner.
		if arg.all_owners && arg.owner.is_some() {
			return Err(tg::error!(
				"the owner and all owners options are mutually exclusive"
			));
		}
		let owner = if arg.all_owners {
			if !matches!(self.context.principal, tg::Principal::Root) {
				return Err(tg::error!("unauthorized"));
			}
			None
		} else if let Some(owner) = arg.owner {
			let owner = self.resolve_runner_owner(&owner).await?;
			let owner = owner.to_id().unwrap();
			self.authorize_runner_owner(Some(&owner)).await?;
			Some(Some(owner))
		} else {
			match &self.context.principal {
				tg::Principal::Root => Some(None),
				tg::Principal::User(user) => Some(Some(user.clone().into())),
				_ => return Err(tg::error!("unauthorized")),
			}
		};

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

		// Read an extra row to determine whether another page exists.
		let mut rows = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.clone();
				let owner = owner.clone();
				async move {
					Self::list_runners_with_transaction(
						transaction,
						owner.as_ref(),
						after.as_ref(),
						limit + 1,
					)
					.await
				}
				.boxed()
			})
			.await?;

		// Create the continuation cursor.
		let cursor = if u64::try_from(rows.len()).unwrap() > limit {
			rows.pop();
			let cursor = Cursor::V0 {
				after: rows.last().unwrap().id.clone(),
			};
			Some(crate::cursor::serialize(&cursor)?)
		} else {
			None
		};

		// Create the output.
		let data = rows
			.into_iter()
			.map(|row| {
				let owner = row.owner.map(Self::runner_owner_from_id).transpose()?;
				Ok(tg::runner::Data {
					created_at: row.created_at,
					id: row.id,
					owner,
				})
			})
			.collect::<tg::Result<_>>()?;

		let output = tg::runner::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_runners_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		owner: Option<&Option<tg::Id>>,
		after: Option<&tg::runner::Id>,
		limit: u64,
	) -> tg::Result<ControlFlow<Vec<Row>, crate::database::Error>> {
		let p = transaction.p();
		let after = after.map(ToString::to_string).unwrap_or_default();
		let mut params = db::params![after, i64::try_from(limit).unwrap()];
		let condition = match owner {
			None => "true".to_owned(),
			Some(None) => "owner is null".to_owned(),
			Some(Some(owner)) => {
				params.extend(db::params![owner.to_string()]);
				format!("owner = {p}3")
			},
		};
		let statement = formatdoc!(
			"
			select created_at, id, owner from runners
			where {condition} and id > {p}1
			order by id limit {p}2;
		"
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), params)
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	pub(crate) async fn list_runners_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let output = self.list_runners(arg).await?;
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(BoxBody::with_bytes(body))
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
