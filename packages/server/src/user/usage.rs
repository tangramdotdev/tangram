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
	pub(crate) async fn try_get_user_usage(
		&self,
		user: &tg::user::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_get_user_usage_regions(user, arg).await
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) == self.server.config.region.as_deref() => {
				self.try_get_user_usage_local(user, arg).await
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.try_get_user_usage_region(user, arg, region).await,
			tg::Location::Remote(remote) => self.try_get_user_usage_remote(user, arg, remote).await,
		}
	}

	async fn try_get_user_usage_regions(
		&self,
		user: &tg::user::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		let Some(account) = self
			.try_resolve_user_usage_account(user, &arg.tokens)
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

	async fn try_get_user_usage_local(
		&self,
		user: &tg::user::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		if !self.server.config.usage.enabled {
			return Err(tg::error!("usage tracking is disabled"));
		}
		let Some(account) = self
			.try_resolve_user_usage_account(user, &arg.tokens)
			.await?
		else {
			return Ok(None);
		};
		let now = self.server.clock.now()?;
		let period = arg.period(now)?;
		let output = self.get_usage_local(&account, period).await?;
		Ok(Some(output))
	}

	async fn try_resolve_user_usage_account(
		&self,
		user: &tg::user::Selector,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::usage::Account>> {
		let permission = tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Admin,
		);
		self.authorize(
			tg::Referent::with_node_and_tokens(user.clone(), tokens.clone()),
			permission,
		)
		.await?
		.into_result()?;

		let user = user.clone();
		let id = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let user = user.clone();
				async move { Self::try_resolve_user_with_transaction(transaction, &user).await }
					.boxed()
			})
			.await?;
		let account = id.map(tg::usage::Account::User);

		Ok(account)
	}

	async fn try_resolve_user_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		user: &tg::user::Selector,
	) -> tg::Result<ControlFlow<Option<tg::user::Id>, crate::database::Error>> {
		let id = match user {
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

	async fn try_get_user_usage_region(
		&self,
		user: &tg::user::Selector,
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
			.try_get_user_usage(user, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to get the usage"))?;
		Ok(output)
	}

	async fn try_get_user_usage_remote(
		&self,
		user: &tg::user::Selector,
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
		let output = client.try_get_user_usage(user, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the usage"),
		)?;

		Ok(output)
	}

	pub(crate) async fn try_get_user_usage_request(
		&self,
		request: http::Request<BoxBody>,
		user: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let user = user
			.replace(':', "/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the user"))?;
		let Some(output) = self.try_get_user_usage(&user, arg).await? else {
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
