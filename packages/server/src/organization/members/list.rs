use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _, response::Ext as _},
};

impl Session {
	pub(crate) async fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::list::Arg,
	) -> tg::Result<tg::organization::members::list::Output> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.list_organization_members_local(organization).await,
			tg::Location::Remote(remote) => {
				self.list_organization_members_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn list_organization_members_local(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<tg::organization::members::list::Output> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Read,
		);
		let authorized = self.authorize(organization.clone(), permission).await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
			return Err(tg::error!("failed to find the organization"));
		}
		let organization = organization.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let organization = organization.clone();
				async move {
					Self::list_organization_members_local_with_transaction(
						transaction,
						&organization,
					)
					.await
				}
				.boxed()
			})
			.await
	}

	async fn list_organization_members_local_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
	) -> tg::Result<ControlFlow<tg::organization::members::list::Output, crate::database::Error>> {
		let id = match organization {
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
		.ok_or_else(|| tg::error!("failed to find the organization"))?;
		let organization =
			match Self::try_get_organization_with_transaction(transaction, &id).await? {
				ControlFlow::Break(organization) => organization,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if organization.is_none() {
			return Err(tg::error!("failed to find the organization"));
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select member
				from organization_members
				where organization = {p}1
				order by member;
			"
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let data = rows
			.into_iter()
			.map(|row| row.member.try_into())
			.collect::<tg::Result<_>>()?;

		Ok(ControlFlow::Break(
			tg::organization::members::list::Output { data },
		))
	}

	async fn list_organization_members_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::list::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::organization::members::list::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.list_organization_members(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to list the organization members"),
			)
	}

	pub(crate) async fn list_organization_members_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
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
		let organization = organization.replace(':', "/").parse()?;
		let output = self.list_organization_members(&organization, arg).await?;
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
