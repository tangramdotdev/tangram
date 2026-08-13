use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

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
		if arg.all && arg.owner.is_some() {
			return Err(tg::error!(
				"the owner and all options are mutually exclusive"
			));
		}
		let owner = if arg.all {
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
		let rows = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let owner = owner.clone();
				async move {
					Self::list_runners_with_transaction(transaction, owner.as_ref()).await
				}
				.boxed()
			})
			.await?;
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

		Ok(tg::runner::list::Output { data })
	}

	async fn list_runners_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		owner: Option<&Option<tg::Id>>,
	) -> tg::Result<ControlFlow<Vec<Row>, crate::database::Error>> {
		let p = transaction.p();
		let (statement, params) = match owner {
			None => (
				"select created_at, id, owner from runners order by id;".into(),
				db::params![],
			),
			Some(None) => (
				"select created_at, id, owner from runners where owner is null order by id;".into(),
				db::params![],
			),
			Some(Some(owner)) => (
				formatdoc!(
					"
						select created_at, id, owner
						from runners
						where owner = {p}1
						order by id;
					"
				)
				.into(),
				db::params![owner.to_string()],
			),
		};
		let result = transaction.query_all_into::<Row>(statement, params).await;
		let rows = crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(rows))
	}

	pub(crate) async fn list_runners_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
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
