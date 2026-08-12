use {
	crate::{Session, location::Remote},
	futures::StreamExt as _,
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) async fn try_get_with_selector(
		&self,
		selector: &tg::Selector<tg::Id>,
		location: Option<&tg::location::Arg>,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let locations = self
			.locations(location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if locations.local.is_some()
			&& let Some(output) = self.try_get_with_selector_local(selector).await?
		{
			return Ok(Some(output));
		}
		let results = locations
			.remotes
			.into_iter()
			.map(|remote| {
				let selector = selector.clone();
				async move {
					let name = remote.name.clone();
					let result = self
						.try_get_with_selector_remote(&selector, remote, cached, ttl)
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
			let result = result
				.map_err(|error| tg::error!(!error, remote = %name, "failed to get the node"))?;
			if output.is_none() {
				output = result;
			}
		}

		Ok(output)
	}

	async fn try_get_with_selector_local(
		&self,
		selector: &tg::Selector<tg::Id>,
	) -> tg::Result<Option<tg::get::Output>> {
		let id = match selector {
			tg::Selector::Id(id) => {
				let mut contains = self
					.contains_ids_from_index(std::slice::from_ref(id))
					.await?;
				contains.pop().unwrap().then(|| id.clone())
			},
			tg::Selector::Specifier(specifier) => {
				let mut ids = self
					.try_get_ids_for_specifiers_from_index(std::slice::from_ref(specifier))
					.await?;
				ids.pop().unwrap()
			},
		};
		let Some(id) = id else {
			return Ok(None);
		};
		let permission = Self::read_permission_for_resource(&id)?;
		let authorized = self
			.authorize(tg::grant::Resource::Id(id.clone()), permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		if !authorized {
			return Ok(None);
		}
		let token = self.create_read_token(&id)?;
		let output = tg::get::Output {
			location: Some(tg::Location::Local(tg::location::Local::default())),
			referent: tg::Referent::with_node_and_token(tg::get::Node::Id(id), token),
		};

		Ok(Some(output))
	}

	async fn try_get_with_selector_remote(
		&self,
		selector: &tg::Selector<tg::Id>,
		remote: Remote,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
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
		let node = match selector {
			tg::Selector::Id(id) => tg::reference::Node::Id(id.clone()),
			tg::Selector::Specifier(specifier) => {
				tg::reference::Node::Specifier(specifier.clone().into())
			},
		};
		let reference = tg::Reference::with_node_and_options(node, options.clone());
		let arg = tg::get::Arg {
			options,
			..tg::get::Arg::default()
		};
		let request = crate::remote::cache::Request::Get(crate::remote::cache::GetRequest {
			arg: arg.clone(),
			reference: reference.clone(),
		});

		// Get a cached response.
		if let Some(crate::remote::cache::Response::Get(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				crate::remote::cache::token_valid(output.referent.token(), &self.server.clock)
			});
			if valid || cached {
				if let Some(output) = &mut output {
					if !crate::remote::cache::token_valid(
						output.referent.token(),
						&self.server.clock,
					) {
						output.referent.options.tokens.remove_local();
					}
					let region = match output.location.as_ref() {
						Some(tg::Location::Local(local)) => local.region.clone(),
						_ => None,
					};
					let location = tg::Location::Remote(tg::location::Remote {
						name: remote.name.clone(),
						region,
					});
					self.update_tokens_for_location(
						&mut output.referent.options.tokens,
						&location,
					)?;
					output.location = Some(location);
				}
				if let Some(output) = &output
					&& !matches!(output.referent.node, tg::get::Node::Id(_))
				{
					return Err(tg::error!("expected an ID"));
				}

				return Ok(output);
			}
		}
		if cached {
			return Ok(None);
		}

		// Get the node from the remote.
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let stream = client
			.try_get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to get the node"))?;
		let mut stream = std::pin::pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		let response = crate::remote::cache::Response::Get(crate::remote::cache::GetResponse {
			output: output.clone(),
		});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		if let Some(output) = &mut output {
			let region = match output.location.as_ref() {
				Some(tg::Location::Local(local)) => local.region.clone(),
				_ => None,
			};
			let location = tg::Location::Remote(tg::location::Remote {
				name: remote.name.clone(),
				region,
			});
			self.update_tokens_for_location(&mut output.referent.options.tokens, &location)?;
			output.location = Some(location);
		}
		if let Some(output) = &output
			&& !matches!(output.referent.node, tg::get::Node::Id(_))
		{
			return Err(tg::error!("expected an ID"));
		}

		Ok(output)
	}
}
