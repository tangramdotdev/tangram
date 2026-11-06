use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_server_core::{
		Session,
		location::{
			Remote, await_first_found_of_local_regions_remotes_futures,
			await_local_future_over_regions_remotes_futures,
		},
	},
};

impl Component {
	pub async fn try_get_with_selector(
		&self,
		session: &Session,
		selector: &tg::Selector<tg::Id>,
		location: Option<&tg::location::Arg>,
		tokens: &tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let mut output = self
			.try_get_with_selector_inner(session, selector, location, tokens, cached, ttl)
			.await?;
		if let Some(output) = &mut output {
			output.referent.options.tokens.inherit(tokens);
		}
		Ok(output)
	}

	pub(crate) async fn try_get_with_selector_inner(
		&self,
		session: &Session,
		selector: &tg::Selector<tg::Id>,
		location: Option<&tg::location::Arg>,
		tokens: &tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let locations = session
			.locations(location)
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Find the node and authorize it without awaiting indexing or searching.
		let node = if current {
			self.try_get_with_selector_id_local(session, selector)
				.await?
		} else {
			None
		};
		if node.is_some()
			&& let Some(output) = self
				.try_get_with_selector_local(
					session,
					selector,
					tokens.local_authorization(),
					node.clone(),
					false,
				)
				.await?
		{
			return Ok(Some(output));
		}

		// Await indexing and search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_get_with_selector_local(
				session,
				selector,
				tokens.local_authorization(),
				node,
				true,
			)
			.await
		};
		let regions = match &locations.local {
			// Every region has the same names, so a local read of a specifier covers the regions.
			Some(_) if current && matches!(selector, tg::Selector::Specifier(_)) => Vec::new(),
			Some(local) => local.regions.clone(),
			None => Vec::new(),
		};
		let remotes = locations.remotes;
		let regions_and_remotes_future = async {
			for region in regions {
				if let Some(output) = self
					.try_get_with_selector_region(session, selector, &region, tokens)
					.await?
				{
					return Ok(Some(output));
				}
			}
			self.try_get_with_selector_remotes(session, selector, remotes, tokens, cached, ttl)
				.await
		};

		// Return the first result for an immutable ID, and prefer the local result for a specifier.
		let output = match selector {
			tg::Selector::Id(_) => {
				await_first_found_of_local_regions_remotes_futures(
					local_future,
					regions_and_remotes_future,
				)
				.boxed()
				.await?
			},
			tg::Selector::Specifier(_) => {
				await_local_future_over_regions_remotes_futures(
					local_future,
					regions_and_remotes_future,
				)
				.boxed()
				.await?
			},
		};

		Ok(output)
	}

	async fn try_get_with_selector_id_local(
		&self,
		session: &Session,
		selector: &tg::Selector<tg::Id>,
	) -> tg::Result<Option<tg::Id>> {
		match selector {
			tg::Selector::Id(id) => {
				let mut contains = session
					.server
					.components
					.node
					.contains_ids_from_index(session, std::slice::from_ref(id))
					.await?;
				Ok(contains.pop().unwrap().then(|| id.clone()))
			},
			tg::Selector::Specifier(specifier) => {
				let mut ids = session
					.server
					.components
					.node
					.try_get_ids_for_specifiers_from_index(session, std::slice::from_ref(specifier))
					.await?;
				Ok(ids.pop().unwrap())
			},
		}
	}

	async fn try_get_with_selector_local(
		&self,
		session: &Session,
		selector: &tg::Selector<tg::Id>,
		tokens: &[tg::authorization::Token],
		node: Option<tg::Id>,
		exhaustive: bool,
	) -> tg::Result<Option<tg::get::Output>> {
		// Find the node if it was not provided, awaiting indexing for a specifier because a forced push can redirect it.
		let mut id = node;
		if id.is_none() && exhaustive {
			if matches!(selector, tg::Selector::Specifier(_)) {
				let arg = tg::index::Arg {
					database: true,
					..Default::default()
				};
				session
					.server
					.components
					.indexer
					.index(session, arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?
					.try_last()
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?;
			}
			id = self
				.try_get_with_selector_id_local(session, selector)
				.await?;
		}
		let Some(id) = id else {
			return Ok(None);
		};

		// Authorize the node.
		let permission = tangram_server_grant_common::read_permission_for_resource(&id)?;
		let resource =
			tg::Referent::with_node_and_local_tokens(tg::Selector::Id(id.clone()), tokens.to_vec());
		let authorized = session
			.authorize_with_exhaustive(resource, permission, exhaustive)
			.await?
			.check_exhaustion()?
			.permissions
			.contains(permission);
		if !authorized {
			return Ok(None);
		}

		// Create the output.
		let mut tokens = tg::authorization::Tokens::with_authorization(tokens.to_vec());
		if let Some(token) = session
			.server
			.components
			.authorization
			.create_read_token(session, &id)?
		{
			tokens.insert_local_authorization(token);
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

	async fn try_get_with_selector_remotes(
		&self,
		session: &Session,
		selector: &tg::Selector<tg::Id>,
		remotes: Vec<Remote>,
		tokens: &tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let results = remotes
			.into_iter()
			.map(|remote| {
				let selector = selector.clone();
				let tokens = tokens.clone();
				async move {
					let name = remote.name.clone();
					let result = self
						.try_get_with_selector_remote(
							session, &selector, remote, &tokens, cached, ttl,
						)
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

	async fn try_get_with_selector_region(
		&self,
		session: &Session,
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
		let client = session
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
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
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
		session: &Session,
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
		let request = tangram_server_remote_core::cache::Request::Get(
			tangram_server_remote_core::cache::GetRequest {
				arg: cache_arg,
				reference: cache_reference,
			},
		);

		// Get a cached response.
		if let Some(tangram_server_remote_core::cache::Response::Get(response)) = session
			.server
			.components
			.remote
			.try_get_cached_remote_response(session, &remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				tangram_server_remote_common::tokens_valid(
					output.referent.local_tokens(),
					&session.server.clock,
				)
			});
			if valid || cached {
				if let Some(output) = &mut output {
					tangram_server_remote_common::remove_expired_tokens(
						&mut output.referent.options.tokens,
						&session.server.clock,
					);
					let region = match output.referent.options.location.as_ref() {
						Some(tg::Location::Local(local)) => local.region.clone(),
						_ => None,
					};
					let location = tg::Location::Remote(tg::location::Remote {
						name: remote.name.clone(),
						region,
					});
					session
						.server
						.components
						.authorization
						.update_tokens_and_location(
							session,
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
		let response = tangram_server_remote_core::cache::Response::Get(
			tangram_server_remote_core::cache::GetResponse {
				output: output.clone(),
			},
		);
		if tokens.is_empty() {
			session
				.server
				.components
				.remote
				.put_cached_remote_response(session, &remote.name, &request, &response)
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
			session
				.server
				.components
				.authorization
				.update_tokens_and_location(
					session,
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
