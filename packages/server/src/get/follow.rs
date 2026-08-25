use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future, stream, stream::BoxStream},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, pin::pin},
	tangram_client::prelude::*,
	tangram_database as db,
};

struct NamedNode {
	id: tg::Id,
	location: Option<tg::Location>,
	specifier: tg::Specifier,
	target: Option<tg::Either<tg::object::Id, tg::process::Id>>,
	tokens: tg::authorization::Tokens,
}

impl Session {
	pub(crate) async fn try_get_with_follow(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>> {
		self.verify_request_with_network_access()?;
		let output = match reference.node() {
			tg::reference::Node::Id(id) => {
				self.try_get_with_follow_id(id, reference.options(), &arg)
					.await?
			},
			tg::reference::Node::Specifier(specifier) => {
				self.try_get_with_follow_specifier(specifier, reference.options(), &arg)
					.await?
			},
			_ => unreachable!(),
		};
		let stream = stream::once(future::ok(tg::progress::Event::Output(output))).boxed();

		Ok(stream)
	}

	async fn try_get_with_follow_id(
		&self,
		id: &tg::Id,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<Option<tg::get::Output>> {
		let output = self
			.try_get_with_selector(
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
		let mut index_output = self
			.try_get_nodes_from_index(std::slice::from_ref(id), &[])
			.await?;
		let specifier = index_output.specifiers.pop().unwrap();
		let Some(specifier) = specifier else {
			return Ok(None);
		};
		let target = if id.kind() == tg::id::Kind::Tag {
			let id = tg::tag::Id::try_from(id.clone())?;
			let Some(output) = self.try_get_tag_local(&id, tokens.clone()).await? else {
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
			.try_get_named_node_target(node, arg.cached, arg.ttl)
			.await?;
		let output = match output {
			None => None,
			Some(output) => {
				self.try_get_apply_get(output, options.get.as_deref())
					.await?
			},
		};

		Ok(output)
	}

	async fn try_get_with_follow_region_id(
		&self,
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
		let client = self
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
			self.update_referent_options_for_location(&mut output.referent.options, &source)?;
		}

		Ok(output)
	}

	async fn try_get_with_follow_remote_id(
		&self,
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
		let request = crate::remote::cache::Request::Get(crate::remote::cache::GetRequest {
			arg: request_arg.clone(),
			reference: reference.clone(),
		});
		if let Some(crate::remote::cache::Response::Get(response)) = self
			.try_get_cached_remote_response(&remote.name, &request, arg.ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
		{
			let mut output = response.output;
			let valid = output.as_ref().is_none_or(|output| {
				crate::remote::cache::token_valid(output.referent.token(), &self.server.clock)
			});
			if valid || arg.cached {
				if let Some(output) = &mut output {
					if !crate::remote::cache::token_valid(
						output.referent.token(),
						&self.server.clock,
					) {
						output.referent.options.tokens.clear();
					}
					let location = tg::Location::Remote(remote.clone());
					self.update_referent_options_for_location(
						&mut output.referent.options,
						&location,
					)?;
				}

				return Ok(output);
			}
		}
		if arg.cached {
			return Ok(None);
		}
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
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
		let response = crate::remote::cache::Response::Get(crate::remote::cache::GetResponse {
			output: output.clone(),
		});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		if let Some(output) = &mut output {
			let location = tg::Location::Remote(remote);
			self.update_referent_options_for_location(&mut output.referent.options, &location)?;
		}

		Ok(output)
	}

	async fn try_get_with_follow_specifier(
		&self,
		specifier: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		arg: &tg::get::Arg,
	) -> tg::Result<Option<tg::get::Output>> {
		let node = self
			.try_get_named_node_for_pattern(specifier, options, arg.cached, arg.ttl)
			.await?;
		let Some(node) = node else {
			return Ok(None);
		};
		let output = if options.follow {
			self.try_get_named_node_target(node, arg.cached, arg.ttl)
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
				self.try_get_apply_get(output, options.get.as_deref())
					.await?
			},
		};

		Ok(output)
	}

	async fn try_get_named_node_for_pattern(
		&self,
		pattern: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<NamedNode>> {
		if !pattern.is_empty() && !pattern.contains_operators() {
			let specifier = pattern.to_specifier();
			let output = self
				.try_get_with_selector(
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
						self.try_get_tag_local(&id, tokens.clone()).await?
					},
					Some(tg::Location::Remote(remote)) => {
						let arg = tg::tag::get::Arg {
							cached,
							location: options.location.clone(),
							tokens: options.tokens.clone(),
							ttl,
						};
						self.try_get_tag_remote(&id, arg, remote, tokens.clone())
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
			groups: true,
			length: Some(1),
			location: options.location.clone(),
			organizations: true,
			pattern: pattern.clone(),
			reverse: true,
			tags: true,
			tokens: options.tokens.clone(),
			ttl,
			users: true,
		};
		let output = self.match_(arg).await.map_err(
			|error| tg::error!(!error, pattern = %pattern_for_error, "failed to match entries"),
		)?;
		let node = output.data.into_iter().next().map(named_node_from_entry);

		Ok(node)
	}

	async fn try_get_named_node_target(
		&self,
		node: NamedNode,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		let tag = if node.id.kind() == tg::id::Kind::Tag {
			node
		} else {
			let tag = self.try_get_top_tag(node, cached, ttl).await?;
			let Some(tag) = tag else {
				return Ok(None);
			};
			tag
		};

		self.try_get_tag_target(tag, cached, ttl).await
	}

	async fn try_get_top_tag(
		&self,
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
			groups: false,
			length: Some(1),
			location: Some(location.clone().into()),
			node: Some(node),
			organizations: false,
			position: None,
			recursive: false,
			reverse: true,
			tags: true,
			ttl,
			users: false,
		};
		let entries = self.list(arg).await?.data;
		let tag = entries
			.into_iter()
			.next()
			.map(named_node_from_entry)
			.map(|mut tag| {
				tag.location = Some(location.clone());
				self.update_tokens_for_location(&mut tag.tokens, &location)?;
				Ok::<_, tg::Error>(tag)
			})
			.transpose()?;

		Ok(tag)
	}

	async fn try_get_tag_target(
		&self,
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
					.try_get_region_tag_target(region, specifier, tokens, cached, ttl)
					.await;
			},
			Some(tg::Location::Remote(remote)) => {
				return self
					.try_get_remote_tag_target(
						target,
						remote.clone(),
						specifier,
						tokens,
						cached,
						ttl,
					)
					.await;
			},
			None | Some(tg::Location::Local(_)) => (),
		}

		let node = list_target_to_id(target);
		let tokens =
			tg::authorization::Tokens::with_local(self.create_tag_target_token(&id, &node).await?);
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
		let client = self
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
			self.update_referent_options_for_location(&mut output.referent.options, &source)?;
		}

		Ok(output)
	}

	async fn try_get_remote_tag_target(
		&self,
		target: tg::Either<tg::object::Id, tg::process::Id>,
		remote: tg::location::Remote,
		specifier: tg::Specifier,
		tokens: tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
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
						output.referent.options.tokens.clear();
					}
					let location = tg::Location::Remote(remote.clone());
					self.update_referent_options_for_location(
						&mut output.referent.options,
						&location,
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
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
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
		let response = crate::remote::cache::Response::Get(crate::remote::cache::GetResponse {
			output: output.clone(),
		});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		let output = output
			.map(|mut output| {
				let location = tg::Location::Remote(remote);
				self.update_referent_options_for_location(&mut output.referent.options, &location)?;
				Ok::<_, tg::Error>(output)
			})
			.transpose()?;

		Ok(output)
	}

	pub(crate) async fn create_tag_target_token(
		&self,
		id: &tg::tag::Id,
		target: &tg::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		// Get the tag.
		let id = id.clone();
		let target = target.clone();
		let session = self.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let id = id.clone();
				let session = session.clone();
				let target = target.clone();
				async move {
					session
						.create_tag_target_token_with_transaction(transaction, &id, &target)
						.await
				}
				.boxed()
			})
			.await
	}

	pub(crate) async fn create_tag_target_token_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::tag::Id,
		target: &tg::Id,
	) -> tg::Result<ControlFlow<Option<tg::authorization::Token>, crate::database::Error>> {
		let data = match Self::get_tag_data_with_transaction(transaction, id).await? {
			ControlFlow::Break(data) => data,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let actual: tg::Id = match data.target {
			tg::tag::data::Target::Object(id) => id.into(),
			tg::tag::data::Target::Process(id) => id.into(),
		};
		if actual != *target {
			return Err(tg::error!("the tag target does not match"));
		}

		let token = self.create_tag_target_token_with_permissions(target, data.permissions)?;

		Ok(ControlFlow::Break(token))
	}

	pub(crate) fn create_tag_target_token_with_permissions(
		&self,
		target: &tg::Id,
		permissions: Vec<tg::authorization::Permission>,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let time_to_live = if target.kind().is_object() {
			self.server.config.object.grant_time_to_live
		} else if target.kind() == tg::id::Kind::Process {
			self.server.config.process.grant_time_to_live
		} else {
			return Err(tg::error!("invalid tag target"));
		};
		let expires_at =
			self.server.clock.unix_timestamp()? + time_to_live.as_secs().to_i64().unwrap();
		let token = self.create_token(target.clone(), permissions, expires_at)?;

		Ok(token)
	}

	pub(crate) async fn match_tags_for_get(
		&self,
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
							self.try_get_tag(&tg::tag::Selector::Id(id), arg).await?
						else {
							return Ok(tg::match_::Output { data: Vec::new() });
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
						return Ok(tg::match_::Output { data: vec![entry] });
					},
					_ => return Ok(tg::match_::Output { data: Vec::new() }),
				}
			}
		}
		let pattern_for_error = pattern.clone();
		let arg = tg::match_::Arg {
			cached,
			groups: false,
			length,
			location: location.cloned(),
			organizations: false,
			pattern,
			reverse: true,
			tags: true,
			tokens: tg::authorization::Tokens::default(),
			ttl,
			users: false,
		};
		let output = self.match_(arg).await.map_err(
			|error| tg::error!(!error, pattern = %pattern_for_error, "failed to match entries"),
		)?;

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
