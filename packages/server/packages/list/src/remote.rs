use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_server_core::{Session, list::remote::Query, location::Remote},
};

pub(crate) type Tasks = tangram_futures::task::Map<
	Key,
	tg::Result<Vec<tg::list::Entry>>,
	tg::Session,
	fnv::FnvBuildHasher,
>;

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
		ttl: tg::remote::cache::Ttl,
		query: Query,
	) -> tg::Result<Vec<tg::list::Entry>> {
		self.list_remote_inner(session, remote, cached, ttl, query)
			.await
	}

	async fn list_remote_inner(
		&self,
		session: &Session,
		remote: Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
		query: Query,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let client = session.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();
		let query = query.with_remote(&remote);
		let request = match &query {
			Query::List(arg) => {
				let arg = arg.clone();
				tangram_server_core::remote::cache::Request::List(
					tangram_server_core::remote::cache::ListRequest { arg },
				)
			},
			Query::Match(arg) => {
				let arg = arg.clone();
				tangram_server_core::remote::cache::Request::Match(
					tangram_server_core::remote::cache::MatchRequest { arg },
				)
			},
		};
		let key = Key {
			principal: session.context.principal.clone(),
			query: query.clone(),
			remote: remote.name.clone(),
		};
		if let Some(response) = session
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut entries = match response {
				tangram_server_core::remote::cache::Response::List(response) => {
					response.output.data
				},
				tangram_server_core::remote::cache::Response::Match(response) => {
					response.output.data
				},
				_ => unreachable!(),
			};
			let valid = entries.iter().all(|entry| {
				let entry_valid = tangram_server_core::remote::cache::tokens_valid(
					entry.tokens().local_authorization(),
					&session.server.clock,
				);
				let target_valid = entry.target.as_ref().is_none_or(|target| {
					tangram_server_core::remote::cache::tokens_valid(
						target.options.tokens.local_authorization(),
						&session.server.clock,
					)
				});
				entry_valid && target_valid
			});
			if valid || cached {
				for entry in &mut entries {
					tangram_server_core::remote::cache::remove_expired_tokens(
						&mut entry.node.options.tokens,
						&session.server.clock,
					);
					if let Some(target) = &mut entry.target {
						tangram_server_core::remote::cache::remove_expired_tokens(
							&mut target.options.tokens,
							&session.server.clock,
						);
					}
					Self::set_remote_entry_location(session, entry, &remote.name, trusted)?;
				}

				return Ok(entries);
			}
		}
		if cached {
			return Ok(Vec::new());
		}
		let task = self.remote_tasks.get_or_spawn_detached_with_context(
			key.clone(),
			|| client,
			move |client, _stop| async move { Self::list_remote_task(key, client).await },
		);
		let trusted = task.context().trusted();
		let entries = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the remote list task panicked"))??;
		let response = match &request {
			tangram_server_core::remote::cache::Request::List(_) => {
				let output = tg::list::Output {
					cursor: None,
					data: entries.clone(),
				};
				let response = tangram_server_core::remote::cache::ListResponse { output };
				tangram_server_core::remote::cache::Response::List(response)
			},
			tangram_server_core::remote::cache::Request::Match(_) => {
				let output = tg::match_::Output {
					cursor: None,
					data: entries.clone(),
				};
				let response = tangram_server_core::remote::cache::MatchResponse { output };
				tangram_server_core::remote::cache::Response::Match(response)
			},
			_ => unreachable!(),
		};
		session
			.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		let entries = entries
			.into_iter()
			.map(|mut entry| {
				Self::set_remote_entry_location(session, &mut entry, &remote.name, trusted)?;
				Ok(entry)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(entries)
	}

	fn set_remote_entry_location(
		session: &Session,
		entry: &mut tg::list::Entry,
		remote: &str,
		trusted: bool,
	) -> tg::Result<()> {
		let region = match entry.location() {
			Some(tg::Location::Local(local)) => local.region.clone(),
			_ => None,
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.to_owned(),
			region,
		});
		session.update_tokens_and_location(
			&mut entry.node.options.tokens,
			Some(&mut entry.node.options.location),
			&location,
			trusted,
		)?;
		if let Some(target) = &mut entry.target {
			session.update_tokens_and_location(
				&mut target.options.tokens,
				Some(&mut target.options.location),
				&location,
				trusted,
			)?;
		}
		Ok(())
	}

	async fn list_remote_task(key: Key, client: tg::Session) -> tg::Result<Vec<tg::list::Entry>> {
		let Key {
			principal: _,
			query,
			remote,
		} = key;
		let data = match query {
			Query::List(arg) => {
				client
					.list_all(arg)
					.await
					.map_err(|error| tg::error!(!error, %remote, "failed to list entries"))?
					.data
			},
			Query::Match(arg) => {
				client
					.match_all(arg)
					.await
					.map_err(|error| tg::error!(!error, %remote, "failed to match entries"))?
					.data
			},
		};

		Ok(data)
	}
}
