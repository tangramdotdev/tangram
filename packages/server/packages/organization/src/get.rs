use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn try_get_organization(
		&self,
		session: &Session,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::get::Arg,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		if arg.location.is_none() {
			let location = session.server.account_location(None)?;
			arg.location = Some(location.into());
		}
		let selector = match organization {
			tg::Selector::Id(id) => tg::Selector::Id(id.clone().into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier.clone()),
		};
		let Some(output) = session
			.server
			.components
			.get
			.try_get_with_selector(
				session,
				&selector,
				arg.location.as_ref(),
				&arg.tokens,
				arg.cached,
				arg.ttl,
			)
			.await?
		else {
			return Ok(None);
		};
		let Some(location) = output.referent.options.location.clone() else {
			return Err(tg::error!("expected a location"));
		};
		let tg::get::Node::Id(id) = output.referent.node else {
			unreachable!();
		};
		let Ok(id) = tg::organization::Id::try_from(id) else {
			return Ok(None);
		};
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_organization_local(session, &id, tokens).await,
			tg::Location::Remote(remote) => {
				self.try_get_organization_remote(session, &id, arg, remote, tokens)
					.await
			},
		}
	}

	pub(crate) async fn try_get_organization_local(
		&self,
		session: &Session,
		id: &tg::organization::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		let permission = tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Read,
		);
		let authorized = session
			.authorize(
				tg::Referent::with_node_and_tokens(
					tg::organization::Selector::Id(id.clone()),
					tokens.clone(),
				),
				permission,
			)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Ok(None);
		}
		let id = id.clone();
		let organization = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_organization_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await?;
		let Some(data) = organization else {
			return Ok(None);
		};
		let output = tg::organization::get::Output {
			data,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
		};

		Ok(Some(output))
	}

	async fn try_get_organization_remote(
		&self,
		session: &Session,
		id: &tg::organization::Id,
		mut arg: tg::organization::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		let cached = arg.cached;
		let cacheable = arg.tokens.is_empty();
		let ttl = arg.ttl;
		let location = tg::Location::Remote(remote.clone());
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		arg.cached = false;
		arg.tokens = arg.tokens.for_location(&location);
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		arg.ttl = tg::remote::cache::Ttl::default();
		let mut cache_arg = arg.clone();
		cache_arg.tokens.clear();
		let request = tangram_server_core::remote::cache::Request::OrganizationGet(
			tangram_server_core::remote::cache::OrganizationGetRequest {
				arg: cache_arg,
				id: id.clone(),
			},
		);
		if let Some(tangram_server_core::remote::cache::Response::OrganizationGet(response)) =
			session
				.server
				.components
				.remote
				.try_get_cached_remote_response(session, &remote.name, &request, ttl)
				.await?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				tangram_server_core::remote::cache::tokens_valid(
					output.tokens.local_authorization(),
					&session.server.clock,
				)
			});
			if valid || cached {
				if let Some(output) = &mut output {
					tangram_server_core::remote::cache::remove_expired_tokens(
						&mut output.tokens,
						&session.server.clock,
					);
					session
						.server
						.components
						.authorization
						.update_tokens_and_location(
							session,
							&mut output.tokens,
							Some(&mut output.location),
							&location,
							trusted,
						)?;
					output.tokens.inherit(&tokens);
				}

				return Ok(output);
			}
		}
		if cached {
			return Ok(None);
		}
		let organization = tg::organization::Selector::Id(id.clone());
		let mut output = client
			.try_get_organization(&organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the organization"),
			)?;
		let response = tangram_server_core::remote::cache::Response::OrganizationGet(
			tangram_server_core::remote::cache::OrganizationGetResponse {
				output: output.clone(),
			},
		);
		if cacheable {
			session
				.server
				.components
				.remote
				.put_cached_remote_response(session, &remote.name, &request, &response)
				.await?;
		}
		if let Some(output) = &mut output {
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
					&mut output.tokens,
					Some(&mut output.location),
					&location,
					trusted,
				)?;
			output.tokens.inherit(&tokens);
		}

		Ok(output)
	}

	pub(crate) async fn try_get_organization_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		organization: &str,
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
		let organization = organization
			.replace(':', "/")
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the organization"))?;
		let Some(output) = self
			.try_get_organization(session, &organization, arg)
			.await?
		else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
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

	pub(crate) async fn try_get_organization_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		id: &tg::organization::Id,
	) -> tg::Result<ControlFlow<Option<tg::organization::Data>, tangram_server_core::database::Error>>
	{
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
		}

		let specifier =
			match tangram_server_core::Session::try_get_specifier_for_id_with_transaction(
				transaction,
				&id.clone().into(),
			)
			.await?
			{
				ControlFlow::Break(specifier) => specifier,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(specifier) = specifier else {
			return Ok(ControlFlow::Break(None));
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select name
				from organizations
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let organization = row.map(|row| tg::organization::Data {
			id: id.clone(),
			name: row.name,
			specifier,
		});

		Ok(ControlFlow::Break(organization))
	}
}
