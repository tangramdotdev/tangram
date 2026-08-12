use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::Index as _,
};

impl Session {
	pub(crate) async fn try_get_organization_usage(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		if !self.server.config.usage.enabled {
			return Err(tg::error!("usage tracking is disabled"));
		}
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		match self.authorize(organization.clone(), permission).await? {
			None => return Ok(None),
			Some(permissions) if permissions.contains(permission) => {},
			Some(_) => return Err(tg::error!("unauthorized")),
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
		let id = match organization {
			tg::Selector::Id(id) => Some(id.clone()),
			tg::Selector::Specifier(specifier) => {
				Self::try_get_id_for_specifier_with_transaction(&transaction, specifier)
					.await?
					.and_then(|id| id.try_into().ok())
			},
		};
		let Some(id) = id else {
			return Ok(None);
		};
		let now = self.server.clock.now()?;
		let period = arg.period(now)?;
		let aggregate = self
			.server
			.index
			.get_usage(&tg::usage::Account::Organization(id.clone()), period, now)
			.await?;
		let output = tg::usage::Output {
			account: id.into(),
			complete: period.end() <= now,
			object_count: aggregate.object_count,
			object_size: aggregate.object_size,
			period: period.range(),
			process_count: aggregate.process_count,
			sandbox_count: aggregate.sandbox_count,
			sandbox_cpu: aggregate.sandbox_cpu,
			sandbox_memory: aggregate.sandbox_memory,
		};

		Ok(Some(output))
	}

	pub(crate) async fn try_get_organization_usage_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let organization = organization.replace(':', "/").parse()?;
		let Some(output) = self.try_get_organization_usage(&organization, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.bytes(body)
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
