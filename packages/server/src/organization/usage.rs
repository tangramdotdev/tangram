use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database as db,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_get_organization_usage(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_get_organization_usage_regions(organization, arg)
					.await
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) == self.server.config.region.as_deref() => {
				self.try_get_organization_usage_local(organization, arg)
					.await
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_get_organization_usage_region(organization, arg, region)
					.await
			},
			tg::Location::Remote(remote) => {
				self.try_get_organization_usage_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn try_get_organization_usage_regions(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		let Some(account) = self
			.try_resolve_organization_usage_account(organization, &arg.tokens)
			.await?
		else {
			return Ok(None);
		};
		let now = self.server.clock.now()?;
		let tokens = arg.tokens.clone();
		let period = arg.period(now)?;
		let output = self.get_usage_regions(&account, period, &tokens).await?;
		Ok(Some(output))
	}

	async fn try_get_organization_usage_local(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		if !self.server.config.usage.enabled {
			return Err(tg::error!("usage tracking is disabled"));
		}
		let Some(account) = self
			.try_resolve_organization_usage_account(organization, &arg.tokens)
			.await?
		else {
			return Ok(None);
		};
		let now = self.server.clock.now()?;
		let period = arg.period(now)?;
		let output = self.get_usage_local(&account, period).await?;
		Ok(Some(output))
	}

	async fn try_resolve_organization_usage_account(
		&self,
		organization: &tg::organization::Selector,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::usage::Account>> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Admin,
		);
		self.authorize(
			tg::Referent::with_node_and_tokens(organization.clone(), tokens.clone()),
			permission,
		)
		.await?
		.into_result()?;

		let organization = organization.clone();
		let id = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let organization = organization.clone();
				async move {
					Self::try_resolve_organization_with_transaction(transaction, &organization)
						.await
				}
				.boxed()
			})
			.await?;
		let account = id.map(tg::usage::Account::Organization);

		Ok(account)
	}

	async fn try_resolve_organization_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
	) -> tg::Result<ControlFlow<Option<tg::organization::Id>, crate::database::Error>> {
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
		};

		Ok(ControlFlow::Break(id))
	}

	async fn try_get_organization_usage_region(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::usage::Arg,
		region: String,
	) -> tg::Result<Option<tg::usage::Output>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		arg.location = Some(location.into());
		let output = client
			.try_get_organization_usage(organization, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to get the usage"))?;
		Ok(output)
	}

	async fn try_get_organization_usage_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::usage::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::usage::Output>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let location = tg::Location::Remote(remote.clone());
		arg.tokens = arg.tokens.for_location(&location);
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region,
			})
			.into(),
		);
		let output = client
			.try_get_organization_usage(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the usage"),
			)?;

		Ok(output)
	}

	pub(crate) async fn try_get_organization_usage_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let organization = organization
			.replace(':', "/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the organization"))?;
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
