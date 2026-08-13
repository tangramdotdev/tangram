use {
	crate::Session,
	futures::{FutureExt as _, Stream, StreamExt as _, future, stream, stream::BoxStream},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, pin::pin},
	tangram_client::prelude::*,
	tangram_database as db,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn try_resolve(
		&self,
		reference: &tg::Reference,
		arg: tg::resolve::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::resolve::Output>>>> + Send + use<>,
	> {
		let stream = if let tg::reference::Node::Specifier(specifier) = reference.node() {
			self.verify_request_with_network_access()?;
			self.try_resolve_specifier(specifier, reference.options(), &arg)
				.await?
		} else {
			let arg = tg::get::Arg {
				cached: arg.cached,
				checkin: arg.checkin,
				options: arg.options,
				ttl: arg.ttl,
			};
			let stream = self.try_get(reference, arg).await?;
			stream
				.map(|result| {
					result.map(|event| match event {
						tg::progress::Event::Diagnostic(diagnostic) => {
							tg::progress::Event::Diagnostic(diagnostic)
						},
						tg::progress::Event::Indicators(indicators) => {
							tg::progress::Event::Indicators(indicators)
						},
						tg::progress::Event::Log(log) => tg::progress::Event::Log(log),
						tg::progress::Event::Output(output) => {
							tg::progress::Event::Output(output.map(|output| tg::resolve::Output {
								referent: output.referent,
							}))
						},
					})
				})
				.boxed()
		};

		Ok(stream)
	}

	async fn try_resolve_specifier(
		&self,
		specifier: &tg::specifier::Pattern,
		options: &tg::reference::Options,
		arg: &tg::resolve::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::resolve::Output>>>>>
	{
		let tg::match_::Output { data } = self
			.match_tags_for_resolve(
				specifier,
				options.location.as_ref(),
				arg.cached,
				Some(1),
				arg.ttl,
			)
			.await?;
		let tag = data.into_iter().find_map(|entry| {
			let tg::match_::Entry::Tag {
				id,
				target,
				location,
				specifier,
				..
			} = entry
			else {
				return None;
			};
			Some((id, target, location, specifier))
		});
		let output = if let Some((id, target, location, specifier)) = tag {
			let output = self
				.try_resolve_tag(id, target, location, specifier, arg.cached, arg.ttl)
				.await?;
			match output {
				None => None,
				Some(output) => self
					.try_get_apply_get(output, options.get.as_deref())
					.await?
					.map(|output| tg::resolve::Output {
						referent: output.referent,
					}),
			}
		} else {
			None
		};
		let stream = stream::once(future::ok(tg::progress::Event::Output(output)));

		Ok(stream.boxed())
	}

	async fn try_resolve_tag(
		&self,
		id: tg::tag::Id,
		target: tg::Either<tg::object::Id, tg::process::Id>,
		location: Option<tg::Location>,
		specifier: tg::Specifier,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		if let Some(tg::Location::Remote(remote)) = location {
			return self
				.try_resolve_remote_tag(target, remote, specifier, cached, ttl)
				.await;
		}

		let node = list_target_to_id(target);
		let tokens =
			tg::authorization::Tokens::with_local(self.create_tag_target_token(&id, &node).await?);
		let referent = tg::Referent::new(
			tg::get::Node::Id(node),
			tg::referent::Options {
				location,
				tag: Some(specifier),
				tokens,
				..tg::referent::Options::default()
			},
		);
		let output = tg::get::Output { referent };

		Ok(Some(output))
	}

	async fn try_resolve_remote_tag(
		&self,
		target: tg::Either<tg::object::Id, tg::process::Id>,
		remote: tg::location::Remote,
		specifier: tg::Specifier,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<tg::get::Output>> {
		// Create the remote request.
		let options = tg::reference::Options {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: remote.region.clone(),
				})
				.into(),
			),
			..tg::reference::Options::default()
		};
		let reference = tg::Reference::with_node_and_options(
			tg::reference::Node::Specifier(specifier.clone().into()),
			options.clone(),
		);
		let arg = tg::resolve::Arg {
			options,
			..tg::resolve::Arg::default()
		};
		let request =
			crate::remote::cache::Request::Resolve(crate::remote::cache::ResolveRequest {
				arg: arg.clone(),
				reference: reference.clone(),
			});

		// Get a cached response.
		if let Some(crate::remote::cache::Response::Resolve(response)) = self
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
			let referent = tg::Referent::new(
				tg::get::Node::Id(list_target_to_id(target)),
				tg::referent::Options {
					location: Some(tg::Location::Remote(remote)),
					tag: Some(specifier),
					..tg::referent::Options::default()
				},
			);
			let output = tg::get::Output { referent };
			return Ok(Some(output));
		}

		// Resolve the tag on the remote.
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let stream = client.try_resolve(&reference, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to resolve the tag"),
		)?;
		let mut stream = pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		let response =
			crate::remote::cache::Response::Resolve(crate::remote::cache::ResolveResponse {
				output: output.clone(),
			});
		self.put_cached_remote_response(&remote.name, &request, &response)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		let output = output
			.map(|mut output| {
				let location = tg::Location::Remote(remote);
				self.update_referent_options_for_location(&mut output.referent.options, &location)?;
				Ok::<_, tg::Error>(tg::get::Output {
					referent: output.referent,
				})
			})
			.transpose()?;

		Ok(output)
	}

	async fn create_tag_target_token(
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

		// Create the token.
		let time_to_live = if target.kind().is_object() {
			self.server.config.object.grant_time_to_live
		} else if target.kind() == tg::id::Kind::Process {
			self.server.config.process.grant_time_to_live
		} else {
			return Err(tg::error!("invalid tag target"));
		};
		let expires_at =
			self.server.clock.unix_timestamp()? + time_to_live.as_secs().to_i64().unwrap();
		let token = self.create_token(target.clone(), data.permissions, expires_at)?;

		Ok(ControlFlow::Break(token))
	}

	pub(super) async fn match_tags_for_resolve(
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
					tg::id::Kind::Group => {
						pattern = tg::specifier::Pattern::any_in_parent(Some(specifier));
					},
					tg::id::Kind::Organization | tg::id::Kind::User => {
						return Ok(tg::match_::Output { data: Vec::new() });
					},
					tg::id::Kind::Tag => {
						let id = tg::tag::Id::try_from(id)?;
						let arg = tg::tag::get::Arg {
							cached,
							location: options.location.map(Into::into),
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
						let entry = tg::list::Entry::Tag {
							id: data.id,
							target,
							location,
							name: data.name,
							parent: data.parent,
							specifier: data.specifier,
							tokens,
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
			ttl,
			users: false,
		};
		let output = self.match_(arg).await.map_err(
			|error| tg::error!(!error, pattern = %pattern_for_error, "failed to match entries"),
		)?;

		Ok(output)
	}

	pub(crate) async fn try_resolve_request(
		&self,
		request: http::Request<BoxBody>,
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let node = path
			.join("/")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the node"))?;
		let arg: tg::resolve::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let reference = tg::Reference::with_node_and_options(node, arg.options.clone());
		let stream = self
			.try_resolve(&reference, arg)
			.await
			.map_err(|error| tg::error!(!error, %reference, "failed to resolve the reference"))?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

fn list_target_to_id(target: tg::Either<tg::object::Id, tg::process::Id>) -> tg::Id {
	match target {
		tg::Either::Left(id) => id.into(),
		tg::Either::Right(id) => id.into(),
	}
}
