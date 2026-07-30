use {
	crate::Session,
	futures::{Stream, StreamExt as _, future, stream, stream::BoxStream},
	num::ToPrimitive as _,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
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
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}
		let stream = if let tg::reference::Item::Specifier(specifier) = reference.item() {
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
								location: output.location,
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
				item,
				location,
				specifier,
				..
			} = entry
			else {
				return None;
			};
			Some((id, item, location, specifier))
		});
		let output = if let Some((id, item, location, specifier)) = tag {
			let output = self
				.try_resolve_tag(id, item, location, specifier, arg.cached, arg.ttl)
				.await?;
			self.try_get_apply_get(output, options.get.as_deref())
				.await?
				.map(|output| tg::resolve::Output {
					location: output.location,
					referent: output.referent,
				})
		} else {
			None
		};
		let stream = stream::once(future::ok(tg::progress::Event::Output(output)));

		Ok(stream.boxed())
	}

	async fn try_resolve_tag(
		&self,
		id: tg::tag::Id,
		item: tg::Either<tg::object::Id, tg::process::Id>,
		location: Option<tg::Location>,
		specifier: tg::Specifier,
		cached: bool,
		ttl: Option<std::time::Duration>,
	) -> tg::Result<tg::get::Output> {
		if let Some(tg::Location::Remote(remote)) = location {
			return self
				.try_resolve_remote_tag(item, remote, specifier, cached, ttl)
				.await;
		}

		let item = list_item_to_id(item);
		let token = self.create_tag_item_token(&id, &item).await?;
		let referent = tg::Referent::new(
			tg::get::Item::Id(item),
			tg::referent::Options {
				tag: Some(specifier),
				token,
				..tg::referent::Options::default()
			},
		);
		let output = tg::get::Output { location, referent };

		Ok(output)
	}

	async fn try_resolve_remote_tag(
		&self,
		item: tg::Either<tg::object::Id, tg::process::Id>,
		remote: tg::location::Remote,
		specifier: tg::Specifier,
		cached: bool,
		ttl: Option<std::time::Duration>,
	) -> tg::Result<tg::get::Output> {
		// Get a cached response.
		let request =
			crate::remote::cache::request("resolve", &(&specifier, remote.region.as_ref()));
		if let Some(mut output) = self
			.try_get_cached_remote_response::<tg::resolve::Output>(&remote.name, &request, ttl)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the remote cache"))?
			.filter(resolve_output_token_valid)
		{
			output.location = Some(tg::Location::Remote(remote));
			let output = tg::get::Output {
				location: output.location,
				referent: output.referent,
			};
			return Ok(output);
		}
		if cached {
			let referent = tg::Referent::new(
				tg::get::Item::Id(list_item_to_id(item)),
				tg::referent::Options {
					tag: Some(specifier),
					..tg::referent::Options::default()
				},
			);
			let output = tg::get::Output {
				location: Some(tg::Location::Remote(remote)),
				referent,
			};
			return Ok(output);
		}

		// Resolve the tag on the remote.
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let options = tg::reference::Options {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: remote.region.clone(),
				})
				.into(),
			),
			..tg::reference::Options::default()
		};
		let reference = tg::Reference::with_item_and_options(
			tg::reference::Item::Specifier(specifier.clone().into()),
			options,
		);
		let stream = client
			.try_resolve(&reference, tg::resolve::Arg::default())
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to resolve the tag"),
			)?;
		let mut stream = pin!(stream);
		let mut output = None;
		while let Some(event) = stream.next().await {
			if let tg::progress::Event::Output(event_output) = event? {
				output = event_output;
			}
		}
		let mut output = output.ok_or_else(|| tg::error!("failed to resolve the tag"))?;
		self.put_cached_remote_response(&remote.name, &request, &output)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the remote cache"))?;
		output.location = Some(tg::Location::Remote(remote));
		let output = tg::get::Output {
			location: output.location,
			referent: output.referent,
		};

		Ok(output)
	}

	async fn create_tag_item_token(
		&self,
		id: &tg::tag::Id,
		item: &tg::Id,
	) -> tg::Result<Option<tg::grant::Token>> {
		// Get the tag.
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let node = Self::try_get_specifier_by_id_with_transaction(&transaction, &id.clone().into())
			.await?
			.ok_or_else(|| tg::error!("failed to find the tag"))?;
		let data = Self::get_tag_data_with_transaction(&transaction, &node).await?;
		let actual: tg::Id = match data.item {
			tg::tag::data::Item::Object(id) => id.into(),
			tg::tag::data::Item::Process(id) => id.into(),
		};
		if actual != *item {
			return Err(tg::error!("the tag item does not match"));
		}

		// Create the token.
		let time_to_live = if item.kind().is_object() {
			self.server.config.object.grant_time_to_live
		} else if item.kind() == tg::id::Kind::Process {
			self.server.config.process.grant_time_to_live
		} else {
			return Err(tg::error!("invalid tag item"));
		};
		let expires_at = time::OffsetDateTime::now_utc().unix_timestamp()
			+ time_to_live.as_secs().to_i64().unwrap();
		let resource = tg::grant::Resource::Id(item.clone());
		let token = self.create_token(resource, data.permissions, expires_at)?;

		Ok(token)
	}

	pub(super) async fn match_tags_for_resolve(
		&self,
		pattern: &tg::specifier::Pattern,
		location: Option<&tg::location::Arg>,
		cached: bool,
		length: Option<u64>,
		ttl: Option<std::time::Duration>,
	) -> tg::Result<tg::match_::Output> {
		let mut pattern = pattern.clone();
		if !pattern.is_empty() && !pattern.contains_operators() {
			let specifier = pattern.to_specifier();
			let entry = self
				.try_get_named_entry(
					&tg::grant::Resource::Specifier(specifier.clone()),
					location,
					cached,
					ttl,
				)
				.await?;
			if let Some(entry) = entry {
				match entry {
					tg::list::Entry::Group { .. } => {
						pattern = tg::specifier::Pattern::any_in_parent(Some(specifier));
					},
					tg::list::Entry::Organization { .. } | tg::list::Entry::User { .. } => {
						return Ok(tg::match_::Output { data: Vec::new() });
					},
					entry @ tg::list::Entry::Tag { .. } => {
						return Ok(tg::match_::Output { data: vec![entry] });
					},
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
		let item = path
			.join("/")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the item"))?;
		let arg: tg::resolve::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let reference = tg::Reference::with_item_and_options(item, arg.options.clone());
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

fn resolve_output_token_valid(output: &tg::resolve::Output) -> bool {
	output.referent.options.token.as_ref().is_none_or(|token| {
		token.body.expires_at > time::OffsetDateTime::now_utc().unix_timestamp()
	})
}

fn list_item_to_id(item: tg::Either<tg::object::Id, tg::process::Id>) -> tg::Id {
	match item {
		tg::Either::Left(id) => id.into(),
		tg::Either::Right(id) => id.into(),
	}
}
