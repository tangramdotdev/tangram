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
		tokens: &tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let locations = self
			.locations(location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_with_selector_local(selector, tokens.local())
					.await?
			{
				return Ok(Some(output));
			}
			for region in local.regions {
				if let Some(output) = self
					.try_get_with_selector_region(selector, &region, tokens)
					.await?
				{
					return Ok(Some(output));
				}
			}
		}
		let results = locations
			.remotes
			.into_iter()
			.map(|remote| {
				let selector = selector.clone();
				let tokens = tokens.clone();
				async move {
					let name = remote.name.clone();
					let result = self
						.try_get_with_selector_remote(&selector, remote, &tokens, cached, ttl)
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
		token: Option<&tg::authorization::Token>,
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
		let resource =
			tg::Referent::with_node_and_token(tg::Selector::Id(id.clone()), token.cloned());
		let authorized = self
			.authorize(resource, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		if !authorized {
			return Ok(None);
		}
		let mut tokens = tg::authorization::Tokens::with_local(token.cloned());
		if let Some(token) = self.create_read_token(&id)? {
			tokens.set_local(token);
		}
		let options = tg::referent::Options {
			location: Some(tg::Location::Local(tg::location::Local::default())),
			tokens,
			..tg::referent::Options::default()
		};
		let referent = tg::Referent::new(tg::get::Node::Id(id), options);
		let output = tg::get::Output { referent };

		Ok(Some(output))
	}

	async fn try_get_with_selector_region(
		&self,
		selector: &tg::Selector<tg::Id>,
		region: &str,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::get::Output>> {
		// Create the region request.
		let source = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let options = tg::reference::Options {
			location: Some(source.clone().into()),
			tokens: tokens.for_location(&source),
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

		// Get the node from the region.
		let client = self
			.get_region_session_for_process(region)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the region client"))?;
		let stream = client
			.try_get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the node"))?;
		let mut stream = std::pin::pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		if let Some(output) = &mut output {
			self.update_tokens_and_location(
				&mut output.referent.options.tokens,
				Some(&mut output.referent.options.location),
				&source,
				false,
			)?;
		}
		if let Some(output) = &output
			&& !matches!(output.referent.node, tg::get::Node::Id(_))
		{
			return Err(tg::error!(%region, "expected an ID"));
		}

		Ok(output)
	}

	async fn try_get_with_selector_remote(
		&self,
		selector: &tg::Selector<tg::Id>,
		remote: Remote,
		tokens: &tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		// Create the remote request.
		let source = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: remote.regions.as_deref().and_then(|regions| match regions {
				[region] => Some(region.clone()),
				_ => None,
			}),
		});
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let trusted = client.trusted();
		let location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let cache_options = tg::reference::Options {
			location: Some(location.clone()),
			..tg::reference::Options::default()
		};
		let node = match selector {
			tg::Selector::Id(id) => tg::reference::Node::Id(id.clone()),
			tg::Selector::Specifier(specifier) => {
				tg::reference::Node::Specifier(specifier.clone().into())
			},
		};
		let cache_reference = tg::Reference::with_node_and_options(node, cache_options.clone());
		let cache_arg = tg::get::Arg {
			options: cache_options,
			..tg::get::Arg::default()
		};
		let request = crate::remote::cache::Request::Get(crate::remote::cache::GetRequest {
			arg: cache_arg,
			reference: cache_reference,
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
					let region = match output.referent.options.location.as_ref() {
						Some(tg::Location::Local(local)) => local.region.clone(),
						_ => None,
					};
					let location = tg::Location::Remote(tg::location::Remote {
						name: remote.name.clone(),
						region,
					});
					self.update_tokens_and_location(
						&mut output.referent.options.tokens,
						Some(&mut output.referent.options.location),
						&location,
						trusted,
					)?;
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
		let options = tg::reference::Options {
			location: Some(location),
			tokens: tokens.for_location(&source),
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
		if tokens.is_empty() {
			self.put_cached_remote_response(&remote.name, &request, &response)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		}
		if let Some(output) = &mut output {
			let region = match output.referent.options.location.as_ref() {
				Some(tg::Location::Local(local)) => local.region.clone(),
				_ => None,
			};
			let location = tg::Location::Remote(tg::location::Remote {
				name: remote.name.clone(),
				region,
			});
			self.update_tokens_and_location(
				&mut output.referent.options.tokens,
				Some(&mut output.referent.options.location),
				&location,
				trusted,
			)?;
		}
		if let Some(output) = &output
			&& !matches!(output.referent.node, tg::get::Node::Id(_))
		{
			return Err(tg::error!("expected an ID"));
		}

		Ok(output)
	}
}
