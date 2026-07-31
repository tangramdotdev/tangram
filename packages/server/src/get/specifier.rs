use {
	crate::{Session, location::Remote},
	futures::{StreamExt as _, TryStreamExt as _},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
};

#[derive(Clone, Debug)]
pub(crate) struct Output {
	pub id: tg::Id,
	pub location: Option<tg::Location>,
	pub token: Option<tg::grant::Token>,
}

struct Source {
	ids: BTreeMap<tg::Specifier, tg::Id>,
	item: Option<Output>,
}

impl Session {
	pub(crate) async fn try_get_specifier(
		&self,
		resource: &tg::grant::Resource,
		location: Option<&tg::location::Arg>,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Output>> {
		let locations = self
			.locations(location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		match resource {
			tg::grant::Resource::Id(id) => {
				self.try_get_specifier_by_id(id, locations, cached, ttl)
					.await
			},
			tg::grant::Resource::Specifier(specifier) => {
				self.try_get_specifier_by_specifier(specifier, locations, cached, ttl)
					.await
			},
		}
	}

	async fn try_get_specifier_by_id(
		&self,
		id: &tg::Id,
		locations: crate::location::Output,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Output>> {
		let resource = tg::grant::Resource::Id(id.clone());
		if locations.local.is_some()
			&& let Some(item) = self.try_get_specifier_local(&resource).await?
		{
			return Ok(Some(item));
		}

		let mut remotes = locations.remotes;
		remotes.sort_by(|a, b| a.name.cmp(&b.name));
		let results = remotes
			.into_iter()
			.map(|remote| {
				let resource = resource.clone();
				async move {
					let name = remote.name.clone();
					let result = self
						.try_get_specifier_remote(&resource, remote, cached, ttl)
						.await;
					(name, result)
				}
			})
			.collect::<futures::stream::FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;
		let mut results = results;
		results.sort_by(|a, b| a.0.cmp(&b.0));
		let mut output = None;
		for (name, result) in results {
			let item = result
				.map_err(|error| tg::error!(!error, remote = %name, "failed to get the item"))?;
			if output.is_none() {
				output = item;
			}
		}

		Ok(output)
	}

	async fn try_get_specifier_by_specifier(
		&self,
		specifier: &tg::Specifier,
		locations: crate::location::Output,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Output>> {
		let specifiers = specifier.prefixes().collect::<Vec<_>>();
		let mut sources = Vec::new();
		if locations.local.is_some() {
			let resource = tg::grant::Resource::Specifier(specifier.clone());
			if let Some(item) = self.try_get_specifier_local(&resource).await? {
				return Ok(Some(item));
			}
			let source = self
				.get_specifier_source_local(&specifiers[..specifiers.len() - 1])
				.await
				.map_err(|error| tg::error!(!error, "failed to get the local items"))?;
			sources.push(source);
		}

		let mut remotes = locations.remotes;
		remotes.sort_by(|a, b| a.name.cmp(&b.name));
		let results = remotes
			.into_iter()
			.map(|remote| {
				let specifiers = specifiers.clone();
				async move {
					let name = remote.name.clone();
					let result = self
						.get_specifier_source_remote(&specifiers, remote, cached, ttl)
						.await;
					(name, result)
				}
			})
			.collect::<futures::stream::FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;
		let mut results = results;
		results.sort_by(|a, b| a.0.cmp(&b.0));
		for (name, result) in results {
			let source = result
				.map_err(|error| tg::error!(!error, remote = %name, "failed to get the items"))?;
			sources.push(source);
		}
		let item = select_specifier(sources, specifier);

		Ok(item)
	}

	async fn get_specifier_source_local(&self, specifiers: &[tg::Specifier]) -> tg::Result<Source> {
		let mut ids = BTreeMap::new();
		for specifier in specifiers {
			let resource = tg::grant::Resource::Specifier(specifier.clone());
			if let Some(output) = self.try_get_specifier_local(&resource).await? {
				ids.insert(specifier.clone(), output.id.clone());
			}
		}
		let source = Source { ids, item: None };

		Ok(source)
	}

	async fn get_specifier_source_remote(
		&self,
		specifiers: &[tg::Specifier],
		remote: Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Source> {
		let results = specifiers
			.iter()
			.cloned()
			.map(|specifier| {
				let remote = remote.clone();
				async move {
					let resource = tg::grant::Resource::Specifier(specifier.clone());
					let item = self
						.try_get_specifier_remote(&resource, remote, cached, ttl)
						.await?;
					Ok::<_, tg::Error>((specifier, item))
				}
			})
			.collect::<futures::stream::FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let target = specifiers.last();
		let mut ids = BTreeMap::new();
		let mut item = None;
		for (specifier, output) in results {
			if let Some(output) = output {
				ids.insert(specifier.clone(), output.id.clone());
				if Some(&specifier) == target {
					item = Some(output);
				}
			}
		}
		let source = Source { ids, item };

		Ok(source)
	}

	async fn try_get_specifier_local(
		&self,
		resource: &tg::grant::Resource,
	) -> tg::Result<Option<Output>> {
		let item =
			{
				let mut connection =
					self.server.database.connection().await.map_err(|error| {
						tg::error!(!error, "failed to get a database connection")
					})?;
				let transaction = connection
					.transaction()
					.await
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				match resource {
					tg::grant::Resource::Id(id) => {
						Self::try_get_specifier_by_id_with_transaction(&transaction, id).await?
					},
					tg::grant::Resource::Specifier(specifier) => {
						Self::try_get_specifier_with_transaction(&transaction, specifier).await?
					},
				}
			};
		let Some(item) = item else {
			return Ok(None);
		};
		let permission = Self::read_permission_for_resource(&item.id)?;
		let authorized = self
			.authorize(tg::grant::Resource::Id(item.id.clone()), permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		if !authorized {
			return Ok(None);
		}
		let token = self.create_read_token(&item.id)?;
		let item = Output {
			id: item.id,
			location: Some(tg::Location::Local(tg::location::Local::default())),
			token,
		};

		Ok(Some(item))
	}

	async fn try_get_specifier_remote(
		&self,
		resource: &tg::grant::Resource,
		remote: Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Output>> {
		// Create the remote request.
		let location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let options = tg::reference::Options {
			location: Some(location),
			..tg::reference::Options::default()
		};
		let item = match resource {
			tg::grant::Resource::Id(id) => tg::reference::Item::Id(id.clone()),
			tg::grant::Resource::Specifier(specifier) => {
				tg::reference::Item::Specifier(specifier.clone().into())
			},
		};
		let reference = tg::Reference::with_item_and_options(item, options.clone());
		let arg = tg::get::Arg {
			options,
			..tg::get::Arg::default()
		};
		let request = crate::remote::cache::Request::Get(crate::remote::cache::GetRequest {
			arg: arg.clone(),
			reference: reference.clone(),
		});

		// Get a cached response.
		if let Some(crate::remote::cache::Response::Get(mut output)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let valid = output
				.as_ref()
				.is_none_or(|output| crate::remote::cache::token_valid(output.referent.token()));
			if valid || cached {
				if let Some(output) = &mut output {
					if !crate::remote::cache::token_valid(output.referent.token()) {
						output.referent.options.token = None;
					}
					set_remote_location(output, &remote.name);
				}

				return output.map(Output::try_from).transpose();
			}
		}
		if cached {
			return Ok(None);
		}

		// Get the item from the remote.
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let stream = client
			.try_get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to get the item"))?;
		let mut stream = std::pin::pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		let response = crate::remote::cache::Response::Get(output.clone());
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		if let Some(output) = &mut output {
			set_remote_location(output, &remote.name);
		}
		let item = output.map(Output::try_from).transpose()?;

		Ok(item)
	}
}

impl TryFrom<tg::get::Output> for Output {
	type Error = tg::Error;

	fn try_from(output: tg::get::Output) -> tg::Result<Self> {
		let tg::get::Item::Id(id) = output.referent.item else {
			return Err(tg::error!("expected an ID"));
		};
		let item = Self {
			id,
			location: output.location,
			token: output.referent.options.token,
		};

		Ok(item)
	}
}

fn select_specifier(sources: Vec<Source>, specifier: &tg::Specifier) -> Option<Output> {
	let mut winners = BTreeMap::<tg::Specifier, tg::Id>::new();
	for source in sources {
		let mut ids = source.ids.iter().collect::<Vec<_>>();
		ids.sort_by(|(a, _), (b, _)| {
			a.components()
				.count()
				.cmp(&b.components().count())
				.then_with(|| a.cmp(b))
		});
		for (specifier, id) in ids {
			if winners.contains_key(specifier) {
				continue;
			}
			let hidden = specifier.ancestors().any(|ancestor| {
				let Some(winner) = winners.get(&ancestor) else {
					return false;
				};
				source.ids.get(&ancestor) != Some(winner)
			});
			if !hidden {
				winners.insert(specifier.clone(), id.clone());
			}
		}
		if let Some(item) = source.item
			&& winners.get(specifier) == Some(&item.id)
		{
			return Some(item);
		}
	}

	None
}

fn set_remote_location(output: &mut tg::get::Output, remote: &str) {
	let region = match output.location.as_ref() {
		Some(tg::Location::Local(local)) => local.region.clone(),
		_ => None,
	};
	output.location = Some(tg::Location::Remote(tg::location::Remote {
		name: remote.to_owned(),
		region,
	}));
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn specifier_allows_a_descendant_with_the_same_ancestor() {
		let parent = tg::group::Id::new();
		let child = tg::group::Id::new();
		let local = source([("foo", parent.clone().into())], None);
		let item = Output {
			id: child.clone().into(),
			location: None,
			token: None,
		};
		let remote = source(
			[("foo", parent.into()), ("foo/a", child.clone().into())],
			Some(item),
		);

		let item = select_specifier(vec![local, remote], &"foo/a".parse().unwrap()).unwrap();

		assert_eq!(item.id, child.into());
	}

	#[test]
	fn specifier_hides_a_descendant_with_a_different_ancestor() {
		let local_parent = tg::group::Id::new();
		let remote_parent = tg::group::Id::new();
		let child = tg::group::Id::new();
		let local = source([("foo", local_parent.into())], None);
		let item = Output {
			id: child.clone().into(),
			location: None,
			token: None,
		};
		let remote = source(
			[("foo", remote_parent.into()), ("foo/a", child.into())],
			Some(item),
		);

		let item = select_specifier(vec![local, remote], &"foo/a".parse().unwrap());

		assert!(item.is_none());
	}

	fn source<const N: usize>(ids: [(&str, tg::Id); N], item: Option<Output>) -> Source {
		let ids = ids
			.into_iter()
			.map(|(specifier, id)| (specifier.parse().unwrap(), id))
			.collect();

		Source { ids, item }
	}
}
