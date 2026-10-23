use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream, stream::BoxStream},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, pin::pin},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

struct NamedNode {
	id: tg::Id,
	location: Option<tg::Location>,
	specifier: tg::Specifier,
	target: Option<tg::Either<tg::object::Id, tg::process::Id>>,
	tokens: tg::authorization::Tokens,
}

struct TryGetRemoteTagTargetArg {
	cached: bool,
	remote: tg::location::Remote,
	specifier: tg::Specifier,
	target: tg::Either<tg::object::Id, tg::process::Id>,
	tokens: tg::authorization::Tokens,
	ttl: tg::remote::cache::Ttl,
}

impl Component {
	pub(crate) async fn try_get_with_follow(
		&self,
		session: &Session,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		session.verify_request_with_network_access()?;
		let output = match reference.node() {
			tg::reference::Node::Id(id) => {
				self.try_get_with_follow_id(session, id, reference.options(), &arg)
					.await?
			},
			tg::reference::Node::Specifier(specifier) => {
				self.try_get_with_follow_specifier(session, specifier, reference.options(), &arg)
					.await?
			},
			_ => unreachable!(),
		};
		let stream = stream::once(future::ok(tg::progress::Event::Output(output))).boxed();

		Ok(stream)
	}

	async fn try_get_with_follow_id(
		&self,
		session: &Session,
		id: &tg::Id,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<Option<tg::get::Output>> {
		let output = self
			.try_get_with_selector(
				session,
				&tg::Selector::Id(id.clone()),
				options.location.as_ref(),
				&options.tokens,
				arg.cached,
				arg.ttl,
			)
			.await?;
		let Some(output) = output else {
			return Ok(None);
		};
		let Some(location) = output.referent.options.location.clone() else {
			return Ok(None);
		};
		match &location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				return self
					.try_get_with_follow_region_id(
						session,
						id,
						options,
						&output.referent.options.tokens,
						region,
						arg,
					)
					.await;
			},
			tg::Location::Remote(remote) => {
				return self
					.try_get_with_follow_remote_id(
						session,
						id,
						options,
						&output.referent.options.tokens,
						remote.clone(),
						arg,
					)
					.await;
			},
			tg::Location::Local(tg::location::Local { region: None }) => (),
		}
		let tokens = output.referent.options.tokens;
		let mut index_output = session
			.server
			.components
			.node
			.try_get_nodes_from_index(session, std::slice::from_ref(id), &[])
			.await?;
		let specifier = index_output.specifiers.pop().unwrap();
		let Some(specifier) = specifier else {
			return Ok(None);
		};
		let target = if id.kind() == tg::id::Kind::Tag {
			let id = tg::tag::Id::try_from(id.clone())?;
			let Some(output) = session
				.server
				.components
				.tag
				.try_get_tag_local(session, &id, tokens.clone())
				.await?
			else {
				return Ok(None);
			};
			Some(match output.data.target {
				tg::tag::data::Target::Object(id) => tg::Either::Left(id),
				tg::tag::data::Target::Process(id) => tg::Either::Right(id),
			})
		} else {
			None
		};
		let node = NamedNode {
			id: id.clone(),
			location: Some(location),
			specifier,
			target,
			tokens,
		};
		let output = self
			.try_get_named_node_target(session, node, arg.cached, arg.ttl)
			.await?;
		let output = match output {
			None => None,
			Some(output) => {
				self.try_get_apply_get(session, output, options.get.as_deref())
					.await?
			},
		};

		Ok(output)
	}

	async fn try_get_with_follow_region_id(
		&self,
		session: &Session,
		id: &tg::Id,
		options: &tg::reference::Options,
		tokens: &tg::authorization::Tokens,
		region: &str,
		arg: &tg::get::Arg,
	) -> tg::Result<Option<tg::get::Output>> {
		let source = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let mut options = options.clone();
		options.follow = true;
		options.tokens = tokens.for_location(&source);
		options.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let reference = tg::Reference::with_node_and_options(
			tg::reference::Node::Id(id.clone()),
			options.clone(),
		);
		let arg = tg::get::Arg {
			cached: arg.cached,
			checkin: arg.checkin.clone(),
			options,
			ttl: arg.ttl,
		};
		let client = session
			.get_region_session(region)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the region client"))?;
		let stream = client
			.try_get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to follow the named node"))?;
		let mut stream = pin!(stream);
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

		Ok(output)
	}

	async fn try_get_with_follow_remote_id(
		&self,
		session: &Session,
		id: &tg::Id,
		options: &tg::reference::Options,
		tokens: &tg::authorization::Tokens,
		remote: tg::location::Remote,
		arg: &tg::get::Arg,
	) -> tg::Result<Option<tg::get::Output>> {
		let mut options = options.clone();
		options.follow = true;
		options.tokens = tokens.for_location(&tg::Location::Remote(remote.clone()));
		options.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		let reference = tg::Reference::with_node_and_options(
			tg::reference::Node::Id(id.clone()),
			options.clone(),
		);
		let request_arg = tg::get::Arg {
			checkin: arg.checkin.clone(),
			options,
			..tg::get::Arg::default()
		};
		let request = tangram_server_core::remote::cache::Request::Get(
			tangram_server_core::remote::cache::GetRequest {
				arg: request_arg.clone(),
				reference: reference.clone(),
			},
		);
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
		if let Some(tangram_server_core::remote::cache::Response::Get(response)) = session
			.server
			.components
			.remote
			.try_get_cached_remote_response(session, &remote.name, &request, arg.ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				tangram_server_core::remote::cache::tokens_valid(
					output.referent.local_tokens(),
					&session.server.clock,
				)
			});
			if valid || arg.cached {
				if let Some(output) = &mut output {
					tangram_server_core::remote::cache::remove_expired_tokens(
						&mut output.referent.options.tokens,
						&session.server.clock,
					);
					let location = tg::Location::Remote(remote.clone());
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

				return Ok(output);
			}
		}
		if arg.cached {
			return Ok(None);
		}
		let stream = client.try_get(&reference, request_arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to follow the named node"),
		)?;
		let mut stream = pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		let response = tangram_server_core::remote::cache::Response::Get(
			tangram_server_core::remote::cache::GetResponse {
				output: output.clone(),
			},
		);
		session
			.server
			.components
			.remote
			.put_cached_remote_response(session, &remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		if let Some(output) = &mut output {
			let location = tg::Location::Remote(remote);
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

		Ok(output)
	}

	async fn try_get_with_follow_specifier(
		&self,
		session: &Session,
		specifier: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<Option<tg::get::Output>> {
		let node = self
			.try_get_named_node_for_pattern(session, specifier, options, arg.cached, arg.ttl)
			.await?;
		let Some(node) = node else {
			return Ok(None);
		};
		let output = if options.follow {
			self.try_get_named_node_target(session, node, arg.cached, arg.ttl)
				.await?
		} else {
			let options = tg::referent::Options {
				location: node.location,
				tokens: node.tokens,
				..tg::referent::Options::default()
			};
			let referent = tg::Referent::new(tg::get::Node::Id(node.id), options);
			Some(tg::get::Output { referent })
		};
		let output = match output {
			None => None,
			Some(output) => {
				self.try_get_apply_get(session, output, options.get.as_deref())
					.await?
			},
		};

		Ok(output)
	}

	async fn try_get_named_node_for_pattern(
		&self,
		session: &Session,
		pattern: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<NamedNode>> {
		if !pattern.is_empty() && !pattern.contains_operators() {
			let specifier = pattern.to_specifier();
			let output = self
				.try_get_with_selector(
					session,
					&tg::Selector::Specifier(specifier.clone()),
					options.location.as_ref(),
					&options.tokens,
					cached,
					ttl,
				)
				.await?;
			let Some(output) = output else {
				return Ok(None);
			};
			let tg::get::Node::Id(id) = output.referent.node else {
				unreachable!();
			};
			if !matches!(
				id.kind(),
				tg::id::Kind::Group
					| tg::id::Kind::Organization
					| tg::id::Kind::Tag
					| tg::id::Kind::User
			) {
				return Ok(None);
			}
			let location = output.referent.options.location;
			let tokens = output.referent.options.tokens;
			let target = if id.kind() == tg::id::Kind::Tag {
				let id = tg::tag::Id::try_from(id.clone())?;
				let output = match location.clone() {
					Some(tg::Location::Local(_)) => {
						session
							.server
							.components
							.tag
							.try_get_tag_local(session, &id, tokens.clone())
							.await?
					},
					Some(tg::Location::Remote(remote)) => {
						let arg = tg::tag::get::Arg {
							cached,
							location: options.location.clone(),
							tokens: options.tokens.clone(),
							ttl,
						};
						session
							.server
							.components
							.tag
							.try_get_tag_remote(session, &id, arg, remote, tokens.clone())
							.await?
					},
					None => return Ok(None),
				};
				let Some(output) = output else {
					return Ok(None);
				};
				Some(match output.data.target {
					tg::tag::data::Target::Object(id) => tg::Either::Left(id),
					tg::tag::data::Target::Process(id) => tg::Either::Right(id),
				})
			} else {
				None
			};
			let node = NamedNode {
				id,
				location,
				specifier,
				target,
				tokens,
			};

			return Ok(Some(node));
		}
		let pattern_for_error = pattern.clone();
		let arg = tg::match_::Arg {
			cached,
			cursor: None,
			groups: true,
			limit: Some(1),
			location: options.location.clone(),
			organizations: true,
			pattern: pattern.clone(),
			reverse: true,
			tags: true,
			tokens: options.tokens.clone(),
			ttl,
			users: true,
		};
		let output = session.match_(arg).await.map_err(
			|error| tg::error!(!error, pattern = %pattern_for_error, "failed to match entries"),
		)?;
		let node = output.data.into_iter().next().map(named_node_from_entry);

		Ok(node)
	}

	async fn try_get_named_node_target(
		&self,
		session: &Session,
		node: NamedNode,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let tag = if node.id.kind() == tg::id::Kind::Tag {
			node
		} else {
			let tag = self.try_get_top_tag(session, node, cached, ttl).await?;
			let Some(tag) = tag else {
				return Ok(None);
			};
			tag
		};

		self.try_get_tag_target(session, tag, cached, ttl).await
	}

	async fn try_get_top_tag(
		&self,
		session: &Session,
		node: NamedNode,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<NamedNode>> {
		let Some(location) = node.location.clone() else {
			return Ok(None);
		};
		let options = tg::referent::Options {
			location: Some(location.clone()),
			tokens: node.tokens,
			..tg::referent::Options::default()
		};
		let node = tg::Referent::new(node.id, options);
		let arg = tg::list::Arg {
			cached,
			cursor: None,
			groups: false,
			limit: Some(1),
			location: Some(location.clone().into()),
			node: Some(node),
			organizations: false,
			recursive: false,
			reverse: true,
			tags: true,
			ttl,
			users: false,
		};
		let entries = session.list(arg).await?.data;
		let tag = entries
			.into_iter()
			.next()
			.map(named_node_from_entry)
			.map(|mut tag| {
				tag.location = Some(location.clone());
				Ok::<_, tg::Error>(tag)
			})
			.transpose()?;

		Ok(tag)
	}

	async fn try_get_tag_target(
		&self,
		session: &Session,
		tag: NamedNode,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let id = tg::tag::Id::try_from(tag.id)?;
		let target = tag
			.target
			.ok_or_else(|| tg::error!(%id, "the tag does not have a target"))?;
		let location = tag.location;
		let specifier = tag.specifier;
		let tokens = tag.tokens;
		match &location {
			Some(tg::Location::Local(tg::location::Local {
				region: Some(region),
			})) => {
				return self
					.try_get_region_tag_target(session, region, specifier, tokens, cached, ttl)
					.await;
			},
			Some(tg::Location::Remote(remote)) => {
				let arg = TryGetRemoteTagTargetArg {
					cached,
					remote: remote.clone(),
					specifier,
					target,
					tokens,
					ttl,
				};
				return self.try_get_remote_tag_target(session, arg).await;
			},
			None | Some(tg::Location::Local(_)) => (),
		}

		let node = list_target_to_id(target);
		let mut target_tokens = tg::authorization::Tokens::with_authorization(
			self.create_tag_target_token(session, &id, &node, &tokens)
				.await?,
		);
		target_tokens.inherit(&tokens);
		let tokens = target_tokens;
		let entry = tg::referent::Options {
			location,
			tag: Some(specifier),
			tokens,
			..tg::referent::Options::default()
		};
		let referent = tg::Referent::new(tg::get::Node::Id(node), entry);
		let output = tg::get::Output { referent };

		Ok(Some(output))
	}

	async fn try_get_region_tag_target(
		&self,
		session: &Session,
		region: &str,
		specifier: tg::Specifier,
		tokens: tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let source = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let options = tg::reference::Options {
			follow: true,
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
			tokens: tokens.for_location(&source),
			..tg::reference::Options::default()
		};
		let reference = tg::Reference::with_node_and_options(
			tg::reference::Node::Specifier(specifier.into()),
			options.clone(),
		);
		let arg = tg::get::Arg {
			cached,
			options,
			ttl,
			..tg::get::Arg::default()
		};
		let client = session
			.get_region_session(region)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the region client"))?;
		let stream = client
			.try_get(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to get the tag target"))?;
		let mut stream = pin!(stream);
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

		Ok(output)
	}

	async fn try_get_remote_tag_target(
		&self,
		session: &Session,
		arg: TryGetRemoteTagTargetArg,
	) -> tg::Result<Option<tg::get::Output>> {
		let TryGetRemoteTagTargetArg {
			cached,
			remote,
			specifier,
			target,
			tokens,
			ttl,
		} = arg;

		// Create the remote request.
		let options = tg::reference::Options {
			follow: true,
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: remote.region.clone(),
				})
				.into(),
			),
			tokens: tokens.for_location(&tg::Location::Remote(remote.clone())),
			..tg::reference::Options::default()
		};
		let reference = tg::Reference::with_node_and_options(
			tg::reference::Node::Specifier(specifier.clone().into()),
			options.clone(),
		);
		let arg = tg::get::Arg {
			options,
			..tg::get::Arg::default()
		};
		let request = tangram_server_core::remote::cache::Request::Get(
			tangram_server_core::remote::cache::GetRequest {
				arg: arg.clone(),
				reference: reference.clone(),
			},
		);
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

		// Get a cached response.
		if let Some(tangram_server_core::remote::cache::Response::Get(response)) = session
			.server
			.components
			.remote
			.try_get_cached_remote_response(session, &remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				tangram_server_core::remote::cache::tokens_valid(
					output.referent.local_tokens(),
					&session.server.clock,
				)
			});
			if valid || cached {
				if let Some(output) = &mut output {
					tangram_server_core::remote::cache::remove_expired_tokens(
						&mut output.referent.options.tokens,
						&session.server.clock,
					);
					let location = tg::Location::Remote(remote.clone());
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
				let output = output.map(|output| tg::get::Output {
					referent: output.referent,
				});

				return Ok(output);
			}
		}
		if cached {
			let entry = tg::referent::Options {
				location: Some(tg::Location::Remote(remote)),
				tag: Some(specifier),
				..tg::referent::Options::default()
			};
			let referent = tg::Referent::new(tg::get::Node::Id(list_target_to_id(target)), entry);
			let output = tg::get::Output { referent };
			return Ok(Some(output));
		}

		// Resolve the tag on the remote.
		let stream = client.try_get(&reference, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the tag target"),
		)?;
		let mut stream = pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		let response = tangram_server_core::remote::cache::Response::Get(
			tangram_server_core::remote::cache::GetResponse {
				output: output.clone(),
			},
		);
		session
			.server
			.components
			.remote
			.put_cached_remote_response(session, &remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		let output = output
			.map(|mut output| {
				let location = tg::Location::Remote(remote);
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
				Ok::<_, tg::Error>(output)
			})
			.transpose()?;

		Ok(output)
	}

	pub async fn create_tag_target_token(
		&self,
		session: &Session,
		id: &tg::tag::Id,
		target: &tg::Id,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<tg::authorization::Token>> {
		// Require permission to read the tag before exposing its target permissions.
		let permission =
			tg::authorization::permission::Set::Tag(tg::authorization::permission::tag::Set::READ);
		let resource = tg::Referent::with_node_and_tokens(
			tg::Selector::<tg::Id>::Id(id.clone().into()),
			tokens.clone(),
		);
		let authorization = session
			.authorize_with_permissions(
				resource,
				permission,
				permission,
				permission.empty_like(),
				None,
			)
			.await?;
		if !authorization.permissions.contains(permission) {
			return Ok(None);
		}

		// Match the indexed permissions to the current target version before searching.
		let Some((actual, version)) = self.try_get_tag_target_state(session, id).await? else {
			return Ok(None);
		};
		if actual != *target {
			return Err(tg::error!(%id, "the tag target does not match"));
		}
		let mut indexed = session.server.index.try_get_tag(id).await?;
		let matches = |tag: &tangram_index::tag::Tag| {
			tag.version == version && list_target_to_id(tag.target.clone()) == *target
		};
		if !indexed.as_ref().is_some_and(matches) {
			session
				.server
				.components
				.indexer
				.index(session)
				.await?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index the tag"))?;
			indexed = session.server.index.try_get_tag(id).await?;
		}
		if !indexed.as_ref().is_some_and(matches) {
			return Ok(None);
		}

		// Search as the tag subject without using the caller's target permissions.
		let (requested, time_to_live) = if target.kind().is_object() {
			let mut permissions = tg::authorization::permission::object::Set::NODE;
			permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
			(
				tg::authorization::permission::Set::Object(permissions),
				session.server.config.object.permission_time_to_live,
			)
		} else if target.kind() == tg::id::Kind::Process {
			let mut permissions = tg::authorization::permission::process::Set::all();
			permissions.remove(tg::authorization::permission::process::Set::PARENT);
			(
				tg::authorization::permission::Set::Process(permissions),
				session.server.config.process.permission_time_to_live,
			)
		} else {
			return Err(tg::error!("invalid tag target"));
		};
		let subject = tg::authorization::Subject::Tag(id.clone());
		let verified = session
			.verify_with_subject(
				target.clone(),
				requested,
				requested.empty_like(),
				tangram_server_core::verify::empty_storage(requested),
				subject,
			)
			.await?;
		if verified.permissions.is_empty() {
			return Ok(None);
		}

		// Discard a proof if the tag changed while verification was pending.
		if self.try_get_tag_target_state(session, id).await? != Some((target.clone(), version)) {
			return Ok(None);
		}
		let expires_at = session
			.server
			.clock
			.unix_timestamp()
			.checked_add(
				i64::try_from(time_to_live.as_secs())
					.map_err(|error| tg::error!(!error, "invalid permission time to live"))?,
			)
			.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
		let expires_at = authorization
			.expires_at
			.into_iter()
			.chain(verified.expires_at)
			.fold(expires_at, i64::min);
		let permissions = verified.permissions.iter().collect();
		let token = session.server.components.authorization.create_token(
			session,
			target.clone(),
			permissions,
			expires_at,
		)?;

		Ok(token)
	}

	async fn try_get_tag_target_state(
		&self,
		session: &Session,
		id: &tg::tag::Id,
	) -> tg::Result<Option<(tg::Id, String)>> {
		let id = id.clone();
		session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				async move {
					Self::try_get_tag_target_state_with_transaction(transaction, &id).await
				}
				.boxed()
			})
			.await
	}

	async fn try_get_tag_target_state_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		id: &tg::tag::Id,
	) -> tg::Result<ControlFlow<Option<(tg::Id, String)>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			target: tg::Id,
			version: String,
		}
		let p = transaction.p();
		let statement = format!("select target, version from tags where id = {p}1;");
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row =
			tangram_server_core::database::retry!(result, "failed to get the tag target state");
		let state = row.map(|row| (row.target, row.version));

		Ok(ControlFlow::Break(state))
	}

	pub(crate) fn create_tag_target_token_with_permissions(
		session: &Session,
		target: &tg::Id,
		permissions: Vec<tg::authorization::Permission>,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let time_to_live = if target.kind().is_object() {
			session.server.config.object.permission_time_to_live
		} else if target.kind() == tg::id::Kind::Process {
			session.server.config.process.permission_time_to_live
		} else {
			return Err(tg::error!("invalid tag target"));
		};
		let expires_at =
			session.server.clock.unix_timestamp() + time_to_live.as_secs().to_i64().unwrap();
		let token = session.server.components.authorization.create_token(
			session,
			target.clone(),
			permissions,
			expires_at,
		)?;

		Ok(token)
	}

	pub async fn match_tags_for_get(
		&self,
		session: &Session,
		pattern: &tg::specifier::Pattern,
		location: Option<&tg::location::Arg>,
		cached: bool,
		length: Option<u64>,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<tg::match_::Output> {
		let mut pattern = pattern.clone();
		if !pattern.is_empty() && !pattern.contains_operators() {
			let specifier = pattern.to_specifier();
			let output = self
				.try_get_with_selector(
					session,
					&tg::Selector::Specifier(specifier.clone()),
					location,
					&tg::authorization::Tokens::default(),
					cached,
					ttl,
				)
				.await?;
			if let Some(output) = output {
				let tg::Referent { node, options } = output.referent;
				let tg::get::Node::Id(id) = node else {
					unreachable!();
				};
				match id.kind() {
					tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User => {
						pattern = tg::specifier::Pattern::any_in_parent(Some(specifier));
					},
					tg::id::Kind::Tag => {
						let id = tg::tag::Id::try_from(id)?;
						let arg = tg::tag::get::Arg {
							cached,
							location: options.location.map(Into::into),
							tokens: options.tokens,
							ttl,
						};
						let Some(output) =
							session.try_get_tag(&tg::tag::Selector::Id(id), arg).await?
						else {
							return Ok(tg::match_::Output {
								cursor: None,
								data: Vec::new(),
							});
						};
						let tg::tag::get::Output {
							data,
							location,
							tokens,
						} = output;
						let target = match data.target {
							tg::tag::data::Target::Object(id) => tg::Either::Left(id),
							tg::tag::data::Target::Process(id) => tg::Either::Right(id),
						};
						let entry = tg::referent::Options {
							location: location.clone(),
							tokens: tokens.clone(),
							..Default::default()
						};
						let target = tg::Referent::new(target, entry);
						let options = tg::referent::Options {
							location,
							tokens,
							..Default::default()
						};
						let node = tg::Referent::new(data.id.into(), options);
						let entry = tg::list::Entry {
							node,
							parent: data.parent,
							specifier: data.specifier,
							target: Some(target),
						};
						return Ok(tg::match_::Output {
							cursor: None,
							data: vec![entry],
						});
					},
					_ => {
						return Ok(tg::match_::Output {
							cursor: None,
							data: Vec::new(),
						});
					},
				}
			}
		}
		let pattern_for_error = pattern.clone();
		let arg = tg::match_::Arg {
			cached,
			cursor: None,
			groups: false,
			limit: None,
			location: location.cloned(),
			organizations: false,
			pattern,
			reverse: true,
			tags: true,
			tokens: tg::authorization::Tokens::default(),
			ttl,
			users: false,
		};
		let mut output = session.match_all(arg).await.map_err(
			|error| tg::error!(!error, pattern = %pattern_for_error, "failed to match entries"),
		)?;

		if let Some(length) = length {
			output
				.data
				.truncate(usize::try_from(length).unwrap_or(usize::MAX));
		}

		Ok(output)
	}
}

fn list_target_to_id(target: tg::Either<tg::object::Id, tg::process::Id>) -> tg::Id {
	match target {
		tg::Either::Left(id) => id.into(),
		tg::Either::Right(id) => id.into(),
	}
}

fn named_node_from_entry(entry: tg::list::Entry) -> NamedNode {
	let tg::list::Entry {
		node,
		parent: _,
		specifier,
		target,
	} = entry;
	NamedNode {
		id: node.node,
		location: node.options.location,
		specifier,
		target: target.map(|target| target.node),
		tokens: node.options.tokens,
	}
}
