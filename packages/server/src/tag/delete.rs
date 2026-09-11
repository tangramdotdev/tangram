use {
	crate::Session,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		self.verify_request_with_network_access()?;
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.delete_tags_primary_region(arg).await
			},
			tg::Location::Local(_) => self.delete_tags_local(arg).await,
			tg::Location::Remote(remote) => self.delete_tags_remote(arg, remote).await,
		}
	}

	async fn delete_tags_local(
		&self,
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

		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		let output = tangram_futures::retry(&options, || {
			let arg = arg.clone();
			let session = session.clone();
			async move {
				match session.delete_tags_local_attempt(&arg).await? {
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		self.server
			.spawn_publish_database_index_outbox_notification_task();
		self.checkout_await_indexing().await?;

		Ok(output)
	}

	async fn delete_tags_local_attempt(
		&self,
		arg: &tg::tag::delete::Arg,
	) -> tg::Result<ControlFlow<tg::tag::delete::Output>> {
		// List the tags before acquiring the write transaction.
		let tags = self
			.list_tags_to_delete(&arg.pattern, arg.recursive)
			.await?;
		let specifiers = tags
			.iter()
			.map(|tag| tag.specifier.clone())
			.collect::<Vec<_>>();
		let ids_by_specifier = self
			.try_get_ids_and_ancestors_for_specifiers(&specifiers)
			.await?;
		for tag in &tags {
			let expected = Some(tg::Id::from(tag.id.clone()));
			if ids_by_specifier.get(&tag.specifier) != Some(&expected) {
				return Ok(ControlFlow::Continue(()));
			}
		}

		// Authorize the tags.
		for tag in &tags {
			let authorized = self
				.authorize(
					tg::Selector::<tg::Id>::Id(tag.id.clone().into()),
					tg::authorization::Permission::Tag(
						tg::authorization::permission::tag::Permission::Write,
					),
				)
				.await?;
			if !authorized.is_some_and(|permissions| {
				permissions.contains(tg::authorization::Permission::Tag(
					tg::authorization::permission::tag::Permission::Write,
				))
			}) {
				return Err(tg::error!("unauthorized"));
			}
		}

		// Delete the tags.
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let ids_by_specifier = ids_by_specifier.clone();
				let session = session.clone();
				let tags = tags.clone();
				async move {
					session
						.delete_tags_local_with_transaction(transaction, tags, &ids_by_specifier)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(output)
	}

	async fn delete_tags_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		tags: Vec<tg::tag::Data>,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
	) -> tg::Result<ControlFlow<ControlFlow<tg::tag::delete::Output>, crate::database::Error>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
		match Self::verify_ids_for_specifiers_with_transaction(
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
			.delete_tags_with_transaction(transaction, &tags, &mut batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		batch.items.extend(
			tags.iter()
				.map(|tag| tangram_index::batch::Item::DeleteTag(tag.id.clone())),
		);
		match self
			.server
			.enqueue_database_index_outbox_with_transaction(transaction, &batch)
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
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::tag::Data>> {
		// List the matching specifiers without holding a database connection.
		let specifiers = if !recursive && !pattern.contains_operators() {
			vec![pattern.clone().try_into()?]
		} else {
			let entries = self
				.list_local_entries()
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
		let tags = self
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
		transaction: &crate::database::Transaction<'_>,
		specifiers: Vec<tg::Specifier>,
	) -> tg::Result<ControlFlow<Vec<tg::tag::Data>, crate::database::Error>> {
		let mut tags = Vec::new();
		for specifier in specifiers {
			let id = match Self::try_get_id_for_specifier_with_transaction(transaction, &specifier)
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
			let tag = match Self::try_get_tag_data_with_transaction(transaction, &id).await? {
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
		transaction: &crate::database::Transaction<'_>,
		tags: &[tg::tag::Data],
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		for tag in tags {
			match self
				.delete_node_grants_with_transaction(transaction, &tag.id.clone().into(), batch)
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
				crate::database::retry!(result, "failed to execute the statement");
			}
		}

		Ok(ControlFlow::Break(()))
	}

	async fn delete_tags_primary_region(
		&self,
		mut arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.delete_tags(arg).await.map_err(|error| {
			tg::error!(!error, "failed to delete the tags in the primary region")
		})?;

		Ok(output)
	}

	async fn delete_tags_remote(
		&self,
		mut arg: tg::tag::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client.delete_tags(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to delete the tags"),
		)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(output)
	}

	pub(crate) async fn delete_tags_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let output = self.delete_tags(arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let body = serde_json::to_vec(&output).unwrap();
				(Some(mime::APPLICATION_JSON), BoxBody::with_bytes(body))
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
