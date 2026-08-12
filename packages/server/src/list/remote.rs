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
		let query = query.with_regions(remote.regions.clone());
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
			let valid = entries
				.iter()
				.all(|entry| crate::remote::cache::token_valid(entry.token(), &self.server.clock));
			if valid || cached {
				for entry in &mut entries {
					if !crate::remote::cache::token_valid(entry.token(), &self.server.clock) {
						entry.set_token(None);
					}
					set_entry_location(entry, &remote.name);
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
				set_entry_location(&mut entry, &remote.name);
				entry
			})
			.collect();

		Ok(entries)
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
	fn with_regions(mut self, regions: Option<Vec<String>>) -> Self {
		let location = Some(tg::location::Arg(vec![
			tg::location::arg::Component::Local(tg::location::arg::LocalComponent { regions }),
		]));
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

fn set_entry_location(entry: &mut tg::list::Entry, remote: &str) {
	let location = entry.location().cloned();
	let region = match location {
		Some(tg::Location::Local(local)) => local.region,
		_ => None,
	};
	let location = Some(tg::Location::Remote(tg::location::Remote {
		name: remote.to_owned(),
		region,
	}));
	match entry {
		tg::list::Entry::Group {
			location: entry_location,
			..
		}
		| tg::list::Entry::Organization {
			location: entry_location,
			..
		}
		| tg::list::Entry::Tag {
			location: entry_location,
			..
		}
		| tg::list::Entry::User {
			location: entry_location,
			..
		} => *entry_location = location,
	}
}
