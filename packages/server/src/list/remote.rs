use {
	crate::{Session, location::Remote},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Query {
	List(tg::list::Arg),
	Match(tg::match_::Arg),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
	pub principal: tg::Principal,
	pub query: Query,
	pub remote: String,
}

pub type Tasks =
	tangram_futures::task::Map<Key, tg::Result<Vec<tg::list::Entry>>, (), fnv::FnvBuildHasher>;

impl Session {
	pub(super) async fn list_remote(
		&self,
		remote: Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
		query: Query,
	) -> tg::Result<Vec<tg::list::Entry>> {
		self.list_remote_inner(remote, cached, ttl, query).await
	}

	async fn list_remote_inner(
		&self,
		remote: Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
		query: Query,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let query = query.with_remote(&remote);
		let request = match &query {
			Query::List(arg) => {
				let arg = arg.clone();
				crate::remote::cache::Request::List(crate::remote::cache::ListRequest { arg })
			},
			Query::Match(arg) => {
				let arg = arg.clone();
				crate::remote::cache::Request::Match(crate::remote::cache::MatchRequest { arg })
			},
		};
		let key = Key {
			principal: self.context.principal.clone(),
			query: query.clone(),
			remote: remote.name.clone(),
		};
		if let Some(response) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut entries = match response {
				crate::remote::cache::Response::List(response) => response.output.data,
				crate::remote::cache::Response::Match(response) => response.output.data,
				_ => unreachable!(),
			};
			let valid = entries.iter().all(|entry| {
				let entry_valid =
					crate::remote::cache::token_valid(entry.tokens().local(), &self.server.clock);
				let target_valid = entry.target.as_ref().is_none_or(|target| {
					crate::remote::cache::token_valid(
						target.options.tokens.local(),
						&self.server.clock,
					)
				});
				entry_valid && target_valid
			});
			if valid || cached {
				for entry in &mut entries {
					if !crate::remote::cache::token_valid(
						entry.tokens().local(),
						&self.server.clock,
					) {
						entry.set_tokens(tg::authorization::Tokens::default());
					}
					if let Some(target) = &mut entry.target
						&& !crate::remote::cache::token_valid(
							target.options.tokens.local(),
							&self.server.clock,
						) {
						target.options.tokens = tg::authorization::Tokens::default();
					}
					self.set_remote_entry_location(entry, &remote.name)?;
				}

				return Ok(entries);
			}
		}
		if cached {
			return Ok(Vec::new());
		}
		let task = self
			.server
			.remote_list_tasks
			.get_or_spawn_detached(key.clone(), {
				let session = self.clone();
				move |_stop| async move { session.list_remote_task(key).await }
			});
		let entries = task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the remote list task panicked"))??;
		let response = match &request {
			crate::remote::cache::Request::List(_) => {
				let output = tg::list::Output {
					data: entries.clone(),
				};
				let response = crate::remote::cache::ListResponse { output };
				crate::remote::cache::Response::List(response)
			},
			crate::remote::cache::Request::Match(_) => {
				let output = tg::match_::Output {
					data: entries.clone(),
				};
				let response = crate::remote::cache::MatchResponse { output };
				crate::remote::cache::Response::Match(response)
			},
			_ => unreachable!(),
		};
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		let entries = entries
			.into_iter()
			.map(|mut entry| {
				self.set_remote_entry_location(&mut entry, &remote.name)?;
				Ok(entry)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(entries)
	}

	fn set_remote_entry_location(
		&self,
		entry: &mut tg::list::Entry,
		remote: &str,
	) -> tg::Result<()> {
		let region = match entry.location() {
			Some(tg::Location::Local(local)) => local.region.clone(),
			_ => None,
		};
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.to_owned(),
			region,
		});
		let mut tokens = entry.tokens().clone();
		self.update_tokens_for_location(&mut tokens, &location)?;
		entry.set_tokens(tokens);
		if let Some(target) = &mut entry.target {
			self.update_referent_options_for_location(&mut target.options, &location)?;
		}
		entry.node.options.location = Some(location);
		Ok(())
	}

	async fn list_remote_task(&self, key: Key) -> tg::Result<Vec<tg::list::Entry>> {
		let Key {
			principal: _,
			query,
			remote,
		} = key;
		let client = self
			.get_remote_session(&remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let data = match query {
			Query::List(arg) => {
				client
					.list(arg)
					.await
					.map_err(|error| tg::error!(!error, %remote, "failed to list entries"))?
					.data
			},
			Query::Match(arg) => {
				client
					.match_(arg)
					.await
					.map_err(|error| tg::error!(!error, %remote, "failed to match entries"))?
					.data
			},
		};

		Ok(data)
	}
}

impl Query {
	#[must_use]
	fn with_remote(mut self, remote: &Remote) -> Self {
		let location = Some(tg::location::Arg(vec![
			tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			}),
		]));
		match &mut self {
			Self::List(arg) => {
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
			Self::Match(arg) => {
				arg.tokens = tokens_for_remote(&arg.tokens, &remote.name);
			},
		}
		let (cached, query_location, ttl) = match &mut self {
			Self::List(arg) => (&mut arg.cached, &mut arg.location, &mut arg.ttl),
			Self::Match(arg) => (&mut arg.cached, &mut arg.location, &mut arg.ttl),
		};
		*cached = false;
		*query_location = location;
		*ttl = tg::remote::cache::Ttl::default();

		self
	}
}

fn tokens_for_remote(
	tokens: &tg::authorization::Tokens,
	remote: &str,
) -> tg::authorization::Tokens {
	let mut output = tg::authorization::Tokens::default();
	for (location, token) in &tokens.0 {
		let tg::Location::Remote(location) = location else {
			continue;
		};
		if location.name == remote {
			let location = tg::Location::Local(tg::location::Local {
				region: location.region.clone(),
			});
			output.insert(location, token.clone());
		}
	}

	output
}
