use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_server_core::{
		Session,
		list::{Cursor, Page, remote::Query},
		location::Remote,
	},
};

pub(crate) type Tasks =
	tangram_futures::task::Map<Key, tg::Result<tg::list::Output>, tg::Session, fnv::FnvBuildHasher>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Key {
	principal: tg::Principal,
	query: Query,
	remote: String,
}

impl Component {
	pub(super) async fn list_remote(
		&self,
		session: &Session,
		remote: Remote,
		cached: bool,
		time_to_live: tg::remote::cache::TimeToLive,
		query: Query,
	) -> tg::Result<Page> {
		self.list_remote_inner(session, remote, cached, time_to_live, query)
			.await
	}

	async fn list_remote_inner(
		&self,
		session: &Session,
		remote: Remote,
		cached: bool,
		time_to_live: tg::remote::cache::TimeToLive,
		query: Query,
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
		let query = query_with_remote(query, &remote);
		let request = match &query {
			Query::List(arg) => {
				let arg = arg.clone();
				tangram_server_remote_core::cache::Request::List(
					tangram_server_remote_core::cache::ListRequest { arg },
				)
			},
			Query::Match(arg) => {
				let arg = arg.clone();
				tangram_server_remote_core::cache::Request::Match(
					tangram_server_remote_core::cache::MatchRequest { arg },
				)
			},
		};
		let key = Key {
			principal: session.context.principal.clone(),
			query: query.clone(),
			remote: remote.name.clone(),
		};
		if let Some(response) = session
			.server
			.components
			.remote
			.try_get_cached_remote_response(session, &remote.name, &request, time_to_live)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let output = match response {
				tangram_server_remote_core::cache::Response::List(response) => response.output,
				tangram_server_remote_core::cache::Response::Match(response) => response.output,
				_ => unreachable!(),
			};
			let next = next_key(output.cursor.as_deref())?;
			let mut items = output.data;
			let valid = items.iter().all(|item| {
				let item_valid = tangram_server_remote_common::tokens_valid(
					item.tokens().local_authorization(),
					&session.server.clock,
				);
				let target_valid = item.target.as_ref().is_none_or(|target| {
					tangram_server_remote_common::tokens_valid(
						target.options.tokens.local_authorization(),
						&session.server.clock,
					)
				});
				item_valid && target_valid
			});
			if valid || cached {
				for item in &mut items {
					tangram_server_remote_common::remove_expired_tokens(
						&mut item.node.options.tokens,
						&session.server.clock,
					);
					if let Some(target) = &mut item.target {
						tangram_server_remote_common::remove_expired_tokens(
							&mut target.options.tokens,
							&session.server.clock,
						);
					}
					Self::set_remote_item_location(session, item, &remote.name, trusted)?;
				}
				let page = Page { data: items, next };

				return Ok(page);
			}
		}
		if cached {
			let page = Page {
				data: Vec::new(),
				next: None,
			};
			return Ok(page);
		}
		let task = self.remote_tasks.get_or_spawn_detached_with_context(
			key.clone(),
			|| client,
			move |client, _stop| async move { Self::list_remote_task(key, client).await },
		);
		let trusted = task.context().trusted();
		let output = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the remote list task panicked"))??;
		let response = match &request {
			tangram_server_remote_core::cache::Request::List(_) => {
				let output = output.clone();
				let response = tangram_server_remote_core::cache::ListResponse { output };
				tangram_server_remote_core::cache::Response::List(response)
			},
			tangram_server_remote_core::cache::Request::Match(_) => {
				let output = output.clone();
				let response = tangram_server_remote_core::cache::MatchResponse { output };
				tangram_server_remote_core::cache::Response::Match(response)
			},
			_ => unreachable!(),
		};
		session
			.server
			.components
			.remote
			.put_cached_remote_response(session, &remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		let next = next_key(output.cursor.as_deref())?;
		let data = output
			.data
			.into_iter()
			.map(|mut item| {
				Self::set_remote_item_location(session, &mut item, &remote.name, trusted)?;
				Ok(item)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let page = Page { data, next };

		Ok(page)
	}

	fn set_remote_item_location(
		session: &Session,
		item: &mut tg::list::Item,
		remote: &str,
		trusted: bool,
	) -> tg::Result<()> {
		let region = match item.location() {
			Some(tg::Location::Local(local)) => local.region.clone(),
			_ => None,
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.to_owned(),
			region,
		});
		session
			.server
			.components
			.authorization
			.update_tokens_and_location(
				session,
				&mut item.node.options.tokens,
				Some(&mut item.node.options.location),
				&location,
				trusted,
			)?;
		if let Some(target) = &mut item.target {
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
					&mut target.options.tokens,
					Some(&mut target.options.location),
					&location,
					trusted,
				)?;
		}
		Ok(())
	}

	async fn list_remote_task(key: Key, client: tg::Session) -> tg::Result<tg::list::Output> {
		let Key {
			principal: _,
			query,
			remote,
		} = key;
		let output = match query {
			Query::List(arg) => client
				.list(arg)
				.await
				.map_err(|error| tg::error!(!error, %remote, "failed to list the items"))?,
			Query::Match(arg) => client
				.match_(arg)
				.await
				.map_err(|error| tg::error!(!error, %remote, "failed to match the items"))?,
		};

		Ok(output)
	}
}

#[must_use]
fn query_with_remote(mut query: Query, remote: &Remote) -> Query {
	let location = Some(tg::location::Arg(vec![
		tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
			regions: remote.regions.clone(),
		}),
	]));
	match &mut query {
		Query::List(arg) => {
			if let Some(node) = &mut arg.node {
				node.options.tokens = tokens_for_remote(&node.options.tokens, &remote.name);
				if let Some(tg::Location::Remote(location)) = &node.options.location
					&& location.name == remote.name
				{
					node.options.location = Some(tg::Location::Local(tg::location::Local {
						region: location.region.clone(),
					}));
				}
			}
		},
		Query::Match(arg) => {
			arg.tokens = tokens_for_remote(&arg.tokens, &remote.name);
		},
	}
	let (cached, query_location, time_to_live) = match &mut query {
		Query::List(arg) => (&mut arg.cached, &mut arg.location, &mut arg.time_to_live),
		Query::Match(arg) => (&mut arg.cached, &mut arg.location, &mut arg.time_to_live),
	};
	*cached = false;
	*query_location = location;
	*time_to_live = tg::remote::cache::TimeToLive::default();

	query
}

#[must_use]
fn tokens_for_remote(
	tokens: &tg::authorization::Tokens,
	remote: &str,
) -> tg::authorization::Tokens {
	let mut output = tg::authorization::Tokens::default();
	for (location, tokens) in tokens.iter() {
		let tg::Location::Remote(location) = location else {
			continue;
		};
		if location.name == remote {
			let location = tg::Location::Local(tg::location::Local {
				region: location.region.clone(),
			});
			output.set(location, tokens.clone());
		}
	}

	output
}

fn next_key(cursor: Option<&str>) -> tg::Result<Option<Vec<u8>>> {
	let next = cursor
		.map(tangram_server_core::cursor::deserialize::<Cursor>)
		.transpose()?
		.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});
	Ok(next)
}
