use {
	crate::Component,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn delete_tags(
		&self,
		session: &Session,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		session.verify_request_with_network_access()?;
		let location = session
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.delete_tags_region(session, arg, region).await
			},
			tg::Location::Local(tg::location::Local { region: None })
				if !session.server.is_primary_region() =>
			{
				self.delete_tags_primary_region(session, arg).await
			},
			tg::Location::Local(_) => self.delete_tags_local(session, arg).await,
			tg::Location::Remote(remote) => self.delete_tags_remote(session, arg, remote).await,
		}
	}

	async fn delete_tags_local(
		&self,
		session: &Session,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		// Validate the pattern.
		if arg.pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
		}
		if !arg.recursive && arg.pattern.contains_operators() {
			return Err(tg::error!(
				"cannot delete multiple tags without --recursive"
			));
		}

		let options = session
			.server
			.config
			.verification
			.write_retry
			.clone()
			.into();
		let component = self.clone();
		let session = session.clone();
		let output = tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let component = component.clone();
			let session = session.clone();
			async move {
				match component.delete_tags_local_attempt(&session, &arg).await? {
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		session
			.server
			.spawn_publish_database_index_queue_notification_task();
		session
			.server
			.components
			.checkout
			.checkout_await_indexing(&session)
			.await?;

		Ok(output)
	}

	async fn delete_tags_local_attempt(
		&self,
		session: &Session,
		arg: &tg::tag::delete::Arg,
	) -> tg::Result<ControlFlow<tg::tag::delete::Output>> {
		// List the tags before acquiring the write transaction.
		let tags = self
			.list_tags_to_delete(session, &arg.pattern, arg.recursive)
			.await?;
		let specifiers = tags
			.iter()
			.map(|tag| tag.specifier.clone())
			.collect::<Vec<_>>();
		let ids_by_specifier = session
			.server
			.components
			.node
			.try_get_ids_and_ancestors_for_specifiers(session, &specifiers)
			.await?;
		for tag in &tags {
			let expected = Some(tg::Id::from(tag.id.clone()));
			if ids_by_specifier.get(&tag.specifier) != Some(&expected) {
				return Ok(ControlFlow::Continue(()));
			}
		}

		// Authorize the tags.
		for tag in &tags {
			let authorized = session
				.authorize(
					tg::Selector::<tg::Id>::Id(tag.id.clone().into()),
					tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Write,
					),
				)
				.await?
				.check_exhaustion()?;
			if !authorized
				.permissions
				.contains(tg::authorization::Permission::Tag(
					tg::authorization::permission::tag::Permission::Write,
				)) {
				return Err(tg::error!("unauthorized"));
			}
		}

		// Delete the tags.
		let component = self.clone();
		let session = session.clone();
		let output = session
			.server
			.database
			.run(|transaction| {
				let ids_by_specifier = ids_by_specifier.clone();
				let component = component.clone();
				let session = session.clone();
				let tags = tags.clone();
				async move {
					component
						.delete_tags_local_with_transaction(
							&session,
							transaction,
							tags,
							&ids_by_specifier,
						)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(output)
	}

	async fn delete_tags_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		tags: Vec<tg::tag::Data>,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<
		ControlFlow<ControlFlow<tg::tag::delete::Output>, tangram_server_core::database::Error>,
	> {
		let batch_size = session.server.config.sync.get.database.batch_size;
		match tangram_server_core::Session::verify_ids_for_specifiers_with_transaction(
			transaction,
			ids_by_specifier,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(true) => (),
			ControlFlow::Break(false) => {
				return Ok(ControlFlow::Break(ControlFlow::Continue(())));
			},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let mut batch = tangram_index::batch::Arg::default();
		match self
			.delete_tags_with_transaction(session, transaction, &tags, &mut batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		batch.items.extend(
			tags.iter()
				.map(|tag| tangram_index::batch::Item::DeleteTag(tag.id.clone())),
		);
		match session
			.server
			.enqueue_database_index_queue_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let output = tg::tag::delete::Output { deleted: tags };

		Ok(ControlFlow::Break(ControlFlow::Break(output)))
	}

	async fn list_tags_to_delete(
		&self,
		session: &Session,
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::tag::Data>> {
		// List the matching specifiers without holding a database connection.
		let specifiers = if !recursive && !pattern.contains_operators() {
			vec![pattern.clone().try_into()?]
		} else {
			let entries = session
				.server
				.components
				.list
				.list_local_entries(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to list the tags"))?;
			entries
				.into_iter()
				.filter_map(|entry| {
					if entry.kind() != tg::id::Kind::Tag {
						return None;
					}
					let specifier = entry.specifier;
					let matches = if recursive {
						specifier
							.prefixes()
							.any(|prefix| pattern.matches_specifier(&prefix))
					} else {
						pattern.matches_specifier(&specifier)
					};
					matches.then_some(specifier)
				})
				.collect()
		};

		// Get the tags in a single transaction.
		let tags = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let specifiers = specifiers.clone();
				async move { Self::list_tags_to_delete_with_transaction(transaction, specifiers).await }
					.boxed()
			})
			.await?;

		Ok(tags)
	}

	async fn list_tags_to_delete_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		specifiers: Vec<tg::Specifier>,
	) -> tg::Result<ControlFlow<Vec<tg::tag::Data>, tangram_server_core::database::Error>> {
		let mut tags = Vec::new();
		for specifier in specifiers {
			let id = match tangram_server_core::Session::try_get_id_for_specifier_with_transaction(
				transaction,
				&specifier,
			)
			.await?
			{
				ControlFlow::Break(id) => id,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let Some(id) = id else {
				continue;
			};
			let Ok(id) = id.try_into() else {
				continue;
			};
			let tag =
				match tangram_server_core::tag::try_get_tag_data_with_transaction(transaction, &id)
					.await?
				{
					ControlFlow::Break(tag) => tag,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
			let Some(tag) = tag else {
				continue;
			};
			tags.push(tag);
		}
		tags.sort_by(|a, b| {
			let a_depth = a.specifier.components().count();
			let b_depth = b.specifier.components().count();
			b_depth
				.cmp(&a_depth)
				.then_with(|| a.specifier.cmp(&b.specifier))
		});

		Ok(ControlFlow::Break(tags))
	}

	async fn delete_tags_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		tags: &[tg::tag::Data],
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		for tag in tags {
			match session
				.server
				.components
				.grant
				.delete_node_grants_with_transaction(
					session,
					transaction,
					&tag.id.clone().into(),
					batch,
				)
				.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			for statement in [
				format!("delete from tags where id = {p}1;"),
				format!("delete from specifiers where id = {p}1;"),
			] {
				let result = transaction
					.execute(statement.into(), db::params![tag.id.to_string()])
					.await;
				tangram_server_core::database::retry!(result, "failed to execute the statement");
			}
		}

		Ok(ControlFlow::Break(()))
	}

	async fn delete_tags_primary_region(
		&self,
		session: &Session,
		mut arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.delete_tags(arg).await.map_err(|error| {
			tg::error!(!error, "failed to delete the tags in the primary region")
		})?;

		Ok(output)
	}

	async fn delete_tags_region(
		&self,
		session: &Session,
		mut arg: tg::tag::delete::Arg,
		region: String,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		let output = client
			.delete_tags(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to delete the tags"))?;

		Ok(output)
	}

	async fn delete_tags_remote(
		&self,
		session: &Session,
		mut arg: tg::tag::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		let output = client.delete_tags(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to delete the tags"),
		)?;
		session
			.server
			.components
			.remote
			.invalidate_remote_cache(session, &remote.name)
			.await;

		Ok(output)
	}

	pub(crate) async fn delete_tags_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the arg"))?;
		let output = self.delete_tags(session, arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let body = serde_json::to_vec(&output).unwrap();
				(Some(mime::APPLICATION_JSON), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
