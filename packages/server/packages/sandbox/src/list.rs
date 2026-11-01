use {
	crate::Component,
	futures::future::try_join_all,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 {
		sandbox: tg::sandbox::Id,
		status: tg::sandbox::Status,
	},
}

struct Page {
	data: Vec<tg::sandbox::get::Output>,
	next: Option<Cursor>,
}

impl Component {
	pub(crate) async fn list_sandboxes(
		&self,
		session: &Session,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let limit = usize::try_from(limit).unwrap();
		let after = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;

		// List a page of the sandboxes in each location.
		let mut pages = Vec::new();
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current {
				let page = self
					.list_sandboxes_local(
						session,
						arg.creator.as_ref(),
						arg.owner.as_ref(),
						arg.status,
						after.clone(),
						limit,
					)
					.await?;
				pages.push(page);
			}
			let region_pages = self
				.list_sandboxes_regions(session, &local.regions, &arg, limit)
				.await?;
			pages.extend(region_pages);
		}
		let remote_pages = self
			.list_sandboxes_remotes(session, &locations.remotes, &arg, limit)
			.await?;
		pages.extend(remote_pages);

		// Merge the pages up to the earliest position that any location has not listed beyond.
		let bound = pages
			.iter()
			.filter_map(|page| page.next.clone())
			.min_by_key(Cursor::key);
		let mut data = pages
			.into_iter()
			.flat_map(|page| page.data)
			.filter(|output| {
				bound
					.as_ref()
					.is_none_or(|bound| output_key(output) <= bound.key())
			})
			.collect::<Vec<_>>();
		data.sort_by_key(output_key);

		// Select the page and create the continuation cursor.
		let next = if data.len() > limit {
			data.truncate(limit);
			data.last().map(|output| Cursor::V0 {
				sandbox: output.data.id.clone(),
				status: output.data.status,
			})
		} else {
			bound
		};
		let cursor = next
			.as_ref()
			.map(tangram_server_core::cursor::serialize)
			.transpose()?;
		let data = data
			.into_iter()
			.map(|output| item(output, arg.expand))
			.collect();
		let output = tg::sandbox::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_sandboxes_local(
		&self,
		session: &Session,
		creator: Option<&tg::Principal>,
		owner: Option<&tg::Principal>,
		status: Option<tg::sandbox::Status>,
		after: Option<Cursor>,
		limit: usize,
	) -> tg::Result<Page> {
		// List a page of the sandboxes from the index by owner when one is given, and by creator otherwise, where the creator defaults to the session's principal.
		let after = after.map(|cursor| match cursor {
			Cursor::V0 { sandbox, status } => {
				tangram_index::sandbox::list::Position { sandbox, status }
			},
		});
		let output = if let Some(owner) = owner {
			let arg = tangram_index::sandbox::list::Arg {
				after,
				limit,
				principal: owner.clone(),
				status,
			};
			session
				.server
				.index
				.list_sandboxes_for_owner(arg)
				.await
				.map_err(
					|error| tg::error!(!error, %owner, "failed to list the sandboxes for the owner"),
				)?
		} else {
			let creator = creator.unwrap_or(&session.context.principal);
			let arg = tangram_index::sandbox::list::Arg {
				after,
				limit,
				principal: creator.clone(),
				status,
			};
			session
				.server
				.index
				.list_sandboxes_for_creator(arg)
				.await
				.map_err(
					|error| tg::error!(!error, %creator, "failed to list the sandboxes for the creator"),
				)?
		};
		let next = output.next.map(|position| Cursor::V0 {
			sandbox: position.sandbox,
			status: position.status,
		});

		// Create the outputs for the sandboxes in this location, keeping only the given creator's sandboxes when listing by owner.
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = output
			.data
			.into_iter()
			.filter_map(|(_, sandbox)| {
				let data = sandbox.data?;
				if owner.is_some() && creator.is_some_and(|creator| data.data.creator != *creator) {
					return None;
				}
				if data
					.location
					.as_ref()
					.is_some_and(|sandbox_location| sandbox_location != &location)
				{
					return None;
				}
				let output = tg::sandbox::get::Output {
					data: data.data,
					location: Some(location.clone()),
					tokens: tg::authorization::Tokens::default(),
				};
				Some(output)
			})
			.collect::<Vec<_>>();

		// Keep the sandboxes the session may read, and add a read token to each.
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Node,
		);
		let authorizations = data
			.iter()
			.map(|output| {
				(
					tg::Selector::Id(tg::Id::from(output.data.id.clone())),
					permission.into(),
				)
			})
			.collect::<Vec<_>>();
		let authorizations = session
			.authorize_batch(authorizations, None)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the sandboxes"))?;
		tangram_server_core::authorization::check_exhaustion(&authorizations)?;
		let mut authorized = Vec::new();
		for (mut output, permissions) in std::iter::zip(data, authorizations) {
			if !permissions.permissions.contains(permission) {
				continue;
			}
			if let Some(token) = session
				.server
				.components
				.authorization
				.create_read_token(session, &output.data.id.clone().into())?
			{
				output.tokens.insert_local_authorization(token);
			}
			authorized.push(output);
		}
		let page = Page {
			data: authorized,
			next,
		};

		Ok(page)
	}

	async fn list_sandboxes_regions(
		&self,
		session: &Session,
		regions: &[String],
		arg: &tg::sandbox::list::Arg,
		limit: usize,
	) -> tg::Result<Vec<Page>> {
		let pages = try_join_all(
			regions
				.iter()
				.map(|region| self.list_sandboxes_region(session, region, arg, limit)),
		)
		.await?;
		Ok(pages)
	}

	async fn list_sandboxes_region(
		&self,
		session: &Session,
		region: &str,
		arg: &tg::sandbox::list::Arg,
		limit: usize,
	) -> tg::Result<Page> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::list::Arg {
			creator: arg.creator.clone(),
			cursor: arg.cursor.clone(),
			expand: true,
			limit: Some(u64::try_from(limit).unwrap()),
			location: Some(location.clone().into()),
			owner: arg.owner.clone(),
			status: arg.status,
		};
		let output = client.list_sandboxes(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to list the sandboxes"),
		)?;
		let mut page = Page::try_from(output)?;
		for output in &mut page.data {
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
					&mut output.tokens,
					Some(&mut output.location),
					&location,
					false,
				)?;
		}
		Ok(page)
	}

	async fn list_sandboxes_remotes(
		&self,
		session: &Session,
		remotes: &[tangram_server_core::location::Remote],
		arg: &tg::sandbox::list::Arg,
		limit: usize,
	) -> tg::Result<Vec<Page>> {
		let pages = try_join_all(
			remotes
				.iter()
				.map(|remote| self.list_sandboxes_remote(session, remote, arg, limit)),
		)
		.await?;
		Ok(pages)
	}

	async fn list_sandboxes_remote(
		&self,
		session: &Session,
		remote: &tangram_server_core::location::Remote,
		arg: &tg::sandbox::list::Arg,
		limit: usize,
	) -> tg::Result<Page> {
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
		let arg = tg::sandbox::list::Arg {
			creator: arg.creator.clone(),
			cursor: arg.cursor.clone(),
			expand: true,
			limit: Some(u64::try_from(limit).unwrap()),
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			owner: arg.owner.clone(),
			status: arg.status,
		};
		let output = client.list_sandboxes(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to list the sandboxes"),
		)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let mut page = Page::try_from(output)?;
		for output in &mut page.data {
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
		}

		Ok(page)
	}

	pub(crate) async fn list_sandboxes_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
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

		let output = self
			.list_sandboxes(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the sandboxes"))?;

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
		let response = response.body(body).unwrap();
		Ok(response)
	}
}

impl Cursor {
	fn key(&self) -> (String, tg::sandbox::Id) {
		match self {
			Self::V0 { sandbox, status } => (status.to_string(), sandbox.clone()),
		}
	}
}

impl TryFrom<tg::sandbox::list::Output> for Page {
	type Error = tg::Error;

	fn try_from(output: tg::sandbox::list::Output) -> tg::Result<Self> {
		let next = output
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;
		let data = output
			.data
			.into_iter()
			.map(|item| match item {
				tg::Either::Left(output) => Ok(output),
				tg::Either::Right(_) => Err(tg::error!("expected an expanded sandbox")),
			})
			.collect::<tg::Result<_>>()?;
		let page = Self { data, next };
		Ok(page)
	}
}

fn output_key(output: &tg::sandbox::get::Output) -> (String, tg::sandbox::Id) {
	(output.data.status.to_string(), output.data.id.clone())
}

fn item(output: tg::sandbox::get::Output, expand: bool) -> tg::sandbox::list::Item {
	if expand {
		return tg::Either::Left(output);
	}
	let options = tg::referent::Options {
		location: output.location,
		tokens: output.tokens,
		..Default::default()
	};
	let referent = tg::Referent {
		node: output.data.id,
		options,
	};
	tg::Either::Right(referent)
}
