use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
	tangram_index::Index as _,
};

impl Session {
	pub(crate) async fn try_get_organization_usage(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<tg::usage::Output>> {
		let permission = tg::grant::Permission::Organization(
			tg::grant::permission::organization::Permission::Admin,
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
		let usage = self
			.server
			.index
			.get_owner_usage(&tangram_index::storage::Owner::Organization(id))
			.await?;
		let output = tg::usage::Output {
			object_count: usage.object_count,
			object_size: usage.object_size,
			process_count: usage.process_count,
		};

		Ok(Some(output))
	}

	pub(crate) async fn try_get_organization_usage_request(
		&self,
		_request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let organization = organization.replace(':', "/").parse()?;
		let Some(output) = self.try_get_organization_usage(&organization).await? else {
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
