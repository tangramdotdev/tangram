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
		process: tg::process::Id,
		status: tg::process::Status,
	},
}

struct Page {
	data: Vec<tg::process::get::Output>,
	next: Option<Cursor>,
}

impl Component {
	pub(crate) async fn list_processes(
		&self,
		session: &Session,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let limit = usize::try_from(limit).unwrap();
		let after = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;

		// List a page of the processes in each location.
		let mut pages = Vec::new();
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current {
				let page = self
					.list_processes_local(
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
				.list_processes_regions(session, &local.regions, &arg, limit)
				.await?;
			pages.extend(region_pages);
		}
		let remote_pages = self
			.list_processes_remotes(session, &locations.remotes, &arg, limit)
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
			.filter(|item| {
				bound
					.as_ref()
					.is_none_or(|bound| item_key(item) <= bound.key())
			})
			.collect::<Vec<_>>();
		data.sort_by_key(item_key);

		// Select the page and create the continuation cursor.
		let next = if data.len() > limit {
			data.truncate(limit);
			let item = data.last().unwrap();
			Some(Cursor::V0 {
				process: item.id.clone(),
				status: item.data.status,
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
		let output = tg::process::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_processes_local(
		&self,
		session: &Session,
		creator: Option<&tg::Principal>,
		owner: Option<&tg::Principal>,
		status: Option<tg::process::Status>,
		after: Option<Cursor>,
		limit: usize,
	) -> tg::Result<Page> {
		// List a page of the processes from the index by owner when one is given, and by creator otherwise, where the creator defaults to the session's principal.
		let after = after.map(|cursor| match cursor {
			Cursor::V0 { process, status } => {
				tangram_index::process::list::Position { process, status }
			},
		});
		let output = if let Some(owner) = owner {
			let arg = tangram_index::process::list::Arg {
				after,
				limit,
				principal: owner.clone(),
				status,
			};
			session
				.server
				.index
				.list_processes_for_owner(arg)
				.await
				.map_err(
					|error| tg::error!(!error, %owner, "failed to list the processes for the owner"),
				)?
		} else {
			let creator = creator.unwrap_or(&session.context.principal);
			let arg = tangram_index::process::list::Arg {
				after,
				limit,
				principal: creator.clone(),
				status,
			};
			session
				.server
				.index
				.list_processes_for_creator(arg)
				.await
				.map_err(
					|error| tg::error!(!error, %creator, "failed to list the processes for the creator"),
				)?
		};
		let next = output.next.map(|position| Cursor::V0 {
			process: position.process,
			status: position.status,
		});

		// Create the outputs for the processes in this location, keeping only the given creator's processes when listing by owner.
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = output
			.data
			.into_iter()
			.filter_map(|(id, process, data)| {
				if owner.is_some() && creator.is_some_and(|creator| data.creator != *creator) {
					return None;
				}
				if process
					.location
					.as_ref()
					.is_some_and(|process_location| process_location != &location)
				{
					return None;
				}
				let output = tg::process::get::Output {
					availability: None,
					data,
					id,
					location: Some(location.clone()),
					metadata: None,
					tokens: tg::authorization::Tokens::default(),
				};
				Some(output)
			})
			.collect::<Vec<_>>();

		// Keep the processes the session may read, and add a read token to each.
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let authorizations = data
			.iter()
			.map(|output| {
				(
					tg::Selector::Id(tg::Id::from(output.id.clone())),
					permission.into(),
				)
			})
			.collect::<Vec<_>>();
		let authorizations = session
			.authorize_batch(authorizations, None)
			.await
			.map_err(|error| tg::error!(!error, "failed to authorize the processes"))?;
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
				.create_read_token(session, &output.id.clone().into())?
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

	async fn list_processes_regions(
		&self,
		session: &Session,
		regions: &[String],
		arg: &tg::process::list::Arg,
		limit: usize,
	) -> tg::Result<Vec<Page>> {
		let pages = try_join_all(
			regions
				.iter()
				.map(|region| self.list_processes_region(session, region, arg, limit)),
		)
		.await?;
		Ok(pages)
	}

	async fn list_processes_region(
		&self,
		session: &Session,
		region: &str,
		arg: &tg::process::list::Arg,
		limit: usize,
	) -> tg::Result<Page> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::list::Arg {
			creator: arg.creator.clone(),
			cursor: arg.cursor.clone(),
			expand: true,
			limit: Some(u64::try_from(limit).unwrap()),
			location: Some(location.clone().into()),
			owner: arg.owner.clone(),
			status: arg.status,
		};
		let output = client.list_processes(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to list the processes"),
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

	async fn list_processes_remotes(
		&self,
		session: &Session,
		remotes: &[tangram_server_core::location::Remote],
		arg: &tg::process::list::Arg,
		limit: usize,
	) -> tg::Result<Vec<Page>> {
		let pages = try_join_all(
			remotes
				.iter()
				.map(|remote| self.list_processes_remote(session, remote, arg, limit)),
		)
		.await?;
		Ok(pages)
	}

	async fn list_processes_remote(
		&self,
		session: &Session,
		remote: &tangram_server_core::location::Remote,
		arg: &tg::process::list::Arg,
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
		let arg = tg::process::list::Arg {
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
		let output = client.list_processes(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to list the processes"),
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

	pub(crate) async fn list_processes_request(
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
			.list_processes(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the processes"))?;

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
	fn key(&self) -> (String, tg::process::Id) {
		match self {
			Self::V0 { process, status } => (status.to_string(), process.clone()),
		}
	}
}

impl TryFrom<tg::process::list::Output> for Page {
	type Error = tg::Error;

	fn try_from(output: tg::process::list::Output) -> tg::Result<Self> {
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
				tg::Either::Right(_) => Err(tg::error!("expected an expanded process")),
			})
			.collect::<tg::Result<_>>()?;
		let page = Self { data, next };
		Ok(page)
	}
}

fn item(output: tg::process::get::Output, expand: bool) -> tg::process::list::Item {
	if expand {
		return tg::Either::Left(output);
	}
	let options = tg::referent::Options {
		location: output.location,
		tokens: output.tokens,
		..Default::default()
	};
	let referent = tg::Referent {
		node: output.id,
		options,
	};
	tg::Either::Right(referent)
}

fn item_key(output: &tg::process::get::Output) -> (String, tg::process::Id) {
	(output.data.status.to_string(), output.id.clone())
}
