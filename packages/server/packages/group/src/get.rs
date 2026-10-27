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
	pub(crate) async fn try_get_group(
		&self,
		session: &Session,
		group: &tg::group::Selector,
		mut arg: tg::group::get::Arg,
	) -> tg::Result<Option<tg::group::get::Output>> {
		if arg.location.is_none() {
			let location = session.server.account_location(None)?;
			arg.location = Some(location.into());
		}
		let selector = match group {
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
		let Ok(id) = tg::group::Id::try_from(id) else {
			return Ok(None);
		};
		let tokens = output.referent.options.tokens;
		match location {
			tg::Location::Local(_) => self.try_get_group_local(session, &id, tokens).await,
			tg::Location::Remote(remote) => {
				self.try_get_group_remote(session, &id, arg, remote, tokens)
					.await
			},
		}
	}

	pub(crate) async fn try_get_group_local(
		&self,
		session: &Session,
		id: &tg::group::Id,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::group::get::Output>> {
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		);
		let authorized = session
			.authorize(
				tg::Referent::with_node_and_tokens(
					tg::group::Selector::Id(id.clone()),
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
		let group = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move { Self::try_get_group_with_transaction(transaction, &id).await }.boxed()
			})
			.await?;
		let Some(data) = group else {
			return Ok(None);
		};
		let output = tg::group::get::Output {
			data,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
		};

		Ok(Some(output))
	}

	async fn try_get_group_remote(
		&self,
		session: &Session,
		id: &tg::group::Id,
		mut arg: tg::group::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<Option<tg::group::get::Output>> {
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
		let request = tangram_server_remote_core::cache::Request::GroupGet(
			tangram_server_remote_core::cache::GroupGetRequest {
				arg: cache_arg,
				id: id.clone(),
			},
		);
		if let Some(tangram_server_remote_core::cache::Response::GroupGet(response)) = session
			.server
			.components
			.remote
			.try_get_cached_remote_response(session, &remote.name, &request, ttl)
			.await?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				tangram_server_remote_core::cache::tokens_valid(
					output.tokens.local_authorization(),
					&session.server.clock,
				)
			});
			if valid || cached {
				if let Some(output) = &mut output {
					tangram_server_remote_core::cache::remove_expired_tokens(
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
		let group = tg::group::Selector::Id(id.clone());
		let mut output = client.try_get_group(&group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the group"),
		)?;
		let response = tangram_server_remote_core::cache::Response::GroupGet(
			tangram_server_remote_core::cache::GroupGetResponse {
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

	pub(crate) async fn try_get_group_request(
		&self,
		session: &Session,
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
		let Some(output) = self.try_get_group(session, &group, arg).await? else {
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

	pub(crate) async fn try_get_group_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		id: &tg::group::Id,
	) -> tg::Result<ControlFlow<Option<tg::group::Data>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
		}

		let specifier =
			match tangram_server_node_common::specifier::try_get_specifier_for_id_with_transaction(
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
				select name, parent
				from groups
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let group = row.map(|row| tg::group::Data {
			id: id.clone(),
			name: row.name,
			parent: row.parent,
			specifier,
		});

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn resolve_parent_for_specifier_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<Option<tg::Id>, tangram_server_core::database::Error>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let Some(parent) = specifier.parent() else {
			return Ok(ControlFlow::Break(None));
		};
		let parent =
			match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
				transaction,
				&parent,
			)
			.await?
			{
				ControlFlow::Break(parent) => parent,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("the parent does not exist"))?;
		if parent.kind() == tg::id::Kind::Tag {
			return Err(tg::error!("a tag cannot be a parent"));
		}

		Ok(ControlFlow::Break(Some(parent)))
	}
}
