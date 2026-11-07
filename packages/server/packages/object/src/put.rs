use {
	crate::Component,
	futures::{FutureExt as _, future},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::{Server, Session},
	tracing::Instrument as _,
};

impl Component {
	pub async fn put_object(
		&self,
		session: &Session,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		let location = session.server.location(arg.location.as_ref())?;

		let (mut output, trusted) = match location.clone() {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => (
				self.put_object_region(session, id, arg, region).await?,
				false,
			),
			tg::Location::Local(_) => (self.put_object_local(session, id, arg).await?, false),
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.put_object_remote(session, id, arg, remote, region)
					.await?
			},
		};
		session
			.server
			.components
			.authorization
			.update_tokens_and_location(
				session,
				&mut output.object.options.tokens,
				Some(&mut output.object.options.location),
				&location,
				trusted,
			)?;

		Ok(output)
	}

	async fn put_object_local(
		&self,
		session: &Session,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		let now = session.server.clock.unix_timestamp();
		let permission_expires_at = now
			+ session
				.server
				.config
				.object
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), arg.bytes.clone())
			.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;

		// Get the children and check the object's limits.
		let mut children = BTreeSet::new();
		data.children(&mut children);
		tg::object::check_limits(arg.bytes.len(), children.len())
			.map_err(|error| tg::error!(!error, %id, "invalid object"))?;

		let length = match &data {
			tg::object::Data::Blob(blob) => Some(blob.length()),
			_ => None,
		};
		let put = uuid::Uuid::now_v7().into_bytes();

		let put_arg = tangram_server_core::cache::object::put::Arg {
			bytes: Some(arg.bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length,
			put,
		};
		let permission = if Self::post_object_batch_authorize(
			session,
			&arg.children,
			&children,
			&BTreeSet::new(),
			&BTreeSet::new(),
		) {
			tg::authorization::permission::object::Permission::Subtree
		} else {
			tg::authorization::permission::object::Permission::Node
		};

		let (node_solvable, node_solved) = match data {
			tg::object::Data::File(file) => match file {
				tg::file::Data::Pointer(_) => (false, true),
				tg::file::Data::Node(node) => (node.solvable(), node.solved()),
			},
			tg::object::Data::Graph(graph) => (graph.solvable(), graph.solved()),
			_ => (false, true),
		};

		let metadata = if let Some(metadata) = arg.metadata {
			metadata
		} else {
			tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: arg.bytes.len().to_u64().unwrap(),
					solvable: node_solvable,
					solved: node_solved,
				},
				..Default::default()
			}
		};
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &session.context.principal)
			.await?;
		let arg = tangram_index::object::put::Arg {
			account,
			checkout: None,
			children,
			id: id.clone(),
			metadata,
			put,
			storage: tg::object::storage::Set::NODE,
			time_to_touch: session.server.config.object.time_to_touch,
			touched_at: now,
		};
		let permission_subject = match &session.context.principal {
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_subject()?),
		};
		let put_permission =
			permission_subject.map(|permission_subject| tangram_index::permission::put::Arg {
				created_at: now,
				creator: Some(session.context.principal.clone()),
				permissions: tg::authorization::Permission::Object(permission).into(),
				resource: id.clone().into(),
				source: tangram_index::permission::Source::Direct {
					expires_at: Some(permission_expires_at),
				},
				subject: permission_subject,
				time_to_touch: Some(session.server.config.object.permission_time_to_touch),
				version: None,
			});
		let arg = tangram_index::batch::Arg {
			items: std::iter::once(tangram_index::batch::Item::PutObject(arg))
				.chain(put_permission.map(tangram_index::batch::Item::PutPermission))
				.collect(),
		};
		self.put_object_and_index(&session.server, put_arg, arg)
			.await?;

		let token = session.server.components.authorization.create_token(
			session,
			id.clone().into(),
			vec![tg::authorization::Permission::Object(permission)],
			permission_expires_at,
		)?;
		let object = tg::Referent::with_node_and_local_tokens(id.clone(), token);

		Ok(tg::object::put::Output { object })
	}

	async fn put_object_region(
		&self,
		session: &Session,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		region: String,
	) -> tg::Result<tg::object::put::Output> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::object::put::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client.put_object(id, arg).await.map_err(
			|error| tg::error!(!error, %id, region = %region, "failed to put the object"),
		)?;
		Ok(output)
	}

	async fn put_object_remote(
		&self,
		session: &Session,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<(tg::object::put::Output, bool)> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote)
			.await
			.map_err(
				|error| tg::error!(!error, %id, remote = %remote, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let arg = tg::object::put::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client.put_object(id, arg).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote, "failed to put the object"),
		)?;
		Ok((output, trusted))
	}

	pub(crate) async fn put_object_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse::<tg::object::Id>()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the object id"))?;
		let (arg, request) = request
			.arg::<tg::object::put::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| {
				tg::error!(argument, !error, "failed to parse the content type header")
			})?;
		let body = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the request body"))?;
		let bytes = match content_type
			.as_ref()
			.map(|content_type| (content_type.type_(), content_type.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => {
				let data = serde_json::from_slice::<tg::object::Data>(&body).map_err(|error| {
					tg::error!(argument, !error, "failed to deserialize the request")
				})?;
				if data.kind() != id.kind() {
					return Err(tg::error!(
						argument,
						expected = %id.kind(),
						actual = %data.kind(),
						"invalid object kind"
					));
				}
				data.serialize()
					.map_err(|error| tg::error!(!error, "failed to serialize the object"))?
			},
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => body,
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid content type"));
			},
		};

		let actual = tg::object::Id::new(id.kind(), &bytes);
		if id != actual {
			let error = tg::error::Data {
				message: Some("invalid object id".into()),
				values: [
					("expected".into(), id.to_string()),
					("actual".into(), actual.to_string()),
				]
				.into(),
				..Default::default()
			};
			let response = http::Response::builder()
				.status(http::StatusCode::BAD_REQUEST)
				.json(error)
				.map_err(|error| tg::error!(!error, "failed to serialize the error"))?
				.unwrap()
				.boxed_body();
			return Ok(response);
		}

		let arg = tg::object::put::Arg { bytes, ..arg };
		let output = self
			.put_object(session, &id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		let response = http::Response::builder()
			.json(output)
			.map_err(|error| tg::error!(!error, "failed to serialize the response"))?
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}

impl Component {
	pub async fn put_object_and_index(
		&self,
		server: &Server,
		object: tangram_server_core::cache::object::put::Arg,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		let future = self
			.put_object_and_index_inner(server, object, index)
			.boxed();
		let result = tokio::time::timeout(server.config.object.put_timeout, future)
			.await
			.map_err(|error| tg::error!(!error, "timed out putting and indexing the object"))?;
		result?;

		Ok(())
	}

	async fn put_object_and_index_inner(
		&self,
		server: &Server,
		object: tangram_server_core::cache::object::put::Arg,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		if !server.config.advanced.multi_server {
			self.put_object_inner(server, object)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the object"))?;
			server
				.components
				.indexer
				.index_batch(server, index)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the object"))?;

			return Ok(());
		}

		// Put the object while enqueueing its archive and index batches in one indexer request.
		let archive = Self::object_archive_batch(server, std::slice::from_ref(&object));
		let arg = tangram_server_core::indexer::EnqueueArg { archive, index };
		let object_put = server.cache.put_object(object);
		let enqueue = server.components.indexer.enqueue(server, arg);
		let (object_result, enqueue_result) = future::join(object_put, enqueue).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the object"))?;
		enqueue_result.map_err(|error| tg::error!(!error, "failed to index the object"))?;

		Ok(())
	}

	pub async fn put_object_with_server(
		&self,
		server: &Server,
		arg: tangram_server_core::cache::object::put::Arg,
	) -> tg::Result<()> {
		let future = self.put_object_inner(server, arg).boxed();
		let result = tokio::time::timeout(server.config.object.put_timeout, future)
			.await
			.map_err(|error| tg::error!(!error, "timed out putting the object"))?;
		result?;

		Ok(())
	}

	async fn put_object_inner(
		&self,
		server: &Server,
		arg: tangram_server_core::cache::object::put::Arg,
	) -> tg::Result<()> {
		if server.archive.is_none() {
			return server
				.cache
				.put_object(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the object"));
		}
		if !server.config.advanced.multi_server {
			return self.put_object_batch_inner(server, vec![arg]).await;
		}

		let archive = Self::object_archive_batch(server, std::slice::from_ref(&arg));
		let enqueue = tangram_server_core::indexer::EnqueueArg {
			archive,
			..Default::default()
		};
		let archive = server.components.indexer.enqueue(server, enqueue);
		let object_put = server.cache.put_object(arg);
		let (object_result, archive_result) = future::join(object_put, archive).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the object"))?;
		archive_result
			.map_err(|error| tg::error!(!error, "failed to enqueue the object for archiving"))?;

		Ok(())
	}

	pub async fn put_object_batch_and_index(
		&self,
		server: &Server,
		objects: Vec<tangram_server_core::cache::object::put::Arg>,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		let future = self
			.put_object_batch_and_index_inner(server, objects, index)
			.boxed();
		let result = tokio::time::timeout(server.config.object.put_timeout, future)
			.await
			.map_err(|error| {
				tg::error!(!error, "timed out putting and indexing the object batch")
			})?;
		result?;

		Ok(())
	}

	async fn put_object_batch_and_index_inner(
		&self,
		server: &Server,
		objects: Vec<tangram_server_core::cache::object::put::Arg>,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		if !server.config.advanced.multi_server {
			self.put_object_batch_inner(server, objects)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
			server
				.components
				.indexer
				.index_batch(server, index)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the object batch"))?;

			return Ok(());
		}

		// Put the objects while enqueueing their archive and index batches in one indexer request.
		let archive = Self::object_archive_batch(server, &objects);
		let arg = tangram_server_core::indexer::EnqueueArg { archive, index };
		let object_put = server.cache.put_object_batch(objects);
		let enqueue = server.components.indexer.enqueue(server, arg);
		let (object_result, enqueue_result) = future::join(object_put, enqueue).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
		enqueue_result.map_err(|error| tg::error!(!error, "failed to index the object batch"))?;

		Ok(())
	}

	#[tracing::instrument(name = "object.put_batch", level = "debug", skip_all, fields(count = args.len(), bytes = args.iter().filter_map(|arg| arg.bytes.as_ref()).map(bytes::Bytes::len).sum::<usize>()), err(level = "debug"))]
	pub async fn put_object_batch(
		&self,
		server: &Server,
		args: Vec<tangram_server_core::cache::object::put::Arg>,
	) -> tg::Result<()> {
		let future = self.put_object_batch_inner(server, args).boxed();
		let result = tokio::time::timeout(server.config.object.put_timeout, future)
			.await
			.map_err(|error| tg::error!(!error, "timed out putting the objects"))?;
		result?;

		Ok(())
	}

	async fn put_object_batch_inner(
		&self,
		server: &Server,
		args: Vec<tangram_server_core::cache::object::put::Arg>,
	) -> tg::Result<()> {
		if server.archive.is_none() {
			return server
				.cache
				.put_object_batch(args)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the objects"));
		}
		if !server.config.advanced.multi_server {
			let archive_args = Self::object_archive_args(&args);
			server
				.cache
				.put_object_batch(args)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
			self.archive_object_batch_task(server, archive_args);
			return Ok(());
		}

		let archive = Self::object_archive_batch(server, &args);
		let enqueue = tangram_server_core::indexer::EnqueueArg {
			archive,
			..Default::default()
		};
		let count = args.len();
		let archive = server
			.components
			.indexer
			.enqueue(server, enqueue)
			.map(|result| {
				tracing::debug!(
					count,
					success = result.is_ok(),
					"completed the archive enqueue batch"
				);
				result
			})
			.instrument(tracing::debug_span!("object.archive_enqueue_batch", count));
		let object_put = server
			.cache
			.put_object_batch(args)
			.map(|result| {
				tracing::debug!(
					count,
					success = result.is_ok(),
					"completed the cache write batch"
				);
				result
			})
			.instrument(tracing::debug_span!("object.cache_write_batch", count));
		let (object_result, archive_result) = future::join(object_put, archive).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
		archive_result
			.map_err(|error| tg::error!(!error, "failed to enqueue the objects for archiving"))?;

		Ok(())
	}

	fn object_archive_batch(
		server: &Server,
		args: &[tangram_server_core::cache::object::put::Arg],
	) -> tangram_server_core::archive::batch::Arg {
		if server.archive.is_none() {
			return tangram_server_core::archive::batch::Arg::default();
		}
		let items = args
			.iter()
			.map(|arg| tangram_server_core::archive::batch::Item {
				object: arg.id.clone(),
				put: arg.put,
			})
			.collect();
		tangram_server_core::archive::batch::Arg { items }
	}

	fn object_archive_args(
		args: &[tangram_server_core::cache::object::put::Arg],
	) -> Vec<tangram_archive::object::put::Arg> {
		args.iter()
			.filter_map(|arg| {
				arg.bytes
					.as_ref()
					.map(|bytes| tangram_archive::object::put::Arg {
						bytes: bytes.clone(),
						id: arg.id.clone(),
						put: arg.put,
					})
			})
			.collect()
	}
}

impl Component {
	pub(crate) fn archive_object_batch_task(
		&self,
		server: &Server,
		args: Vec<tangram_archive::object::put::Arg>,
	) {
		if args.is_empty() {
			return;
		}
		self.archive_tasks
			.spawn({
				let server = server.clone();
				|_| async move {
					let result = Self::archive_object_batch(&server, args).await;
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "failed to archive the objects");
					}
					result
				}
			})
			.detach();
	}

	pub(crate) async fn archive_object_batch(
		server: &Server,
		args: Vec<tangram_archive::object::put::Arg>,
	) -> tg::Result<()> {
		let archive = server
			.archive
			.as_ref()
			.ok_or_else(|| tg::error!("the archive is unavailable"))?;

		// Put the objects in the archive.
		let entries = args
			.iter()
			.map(|arg| (arg.id.clone(), arg.put))
			.collect::<Vec<_>>();
		archive
			.put_object_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the objects in the archive"))?;

		// Put the object cache entries.
		if let Some(config) = &server.config.object.cache {
			future::try_join_all(entries.into_iter().map(|(id, put)| {
				let arg = tangram_server_core::cache::object::cache::put::Arg {
					cache: uuid::Uuid::now_v7().into_bytes(),
					id,
					partition: rand::random_range(0..config.partition_total),
					put,
				};
				server.cache.put_object_cache_entry(arg)
			}))
			.await?;
		}

		Ok(())
	}
}
