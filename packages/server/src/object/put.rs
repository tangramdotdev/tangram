use {
	crate::{Server, Session},
	futures::{FutureExt as _, future},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::Messenger as _,
	tangram_store::prelude::*,
};

impl Session {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		let location = self.server.location(arg.location.as_ref())?;

		let (mut output, trusted) = match location.clone() {
			tg::Location::Local(tg::location::Local { region: None }) => {
				(self.put_object_local(id, arg).await?, false)
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => (self.put_object_region(id, arg, region).await?, false),
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.put_object_remote(id, arg, remote, region).await?,
		};
		self.update_tokens_and_location(
			&mut output.object.options.tokens,
			Some(&mut output.object.options.location),
			&location,
			trusted,
		)?;

		Ok(output)
	}

	async fn put_object_local(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		let now = self.server.clock.unix_timestamp()?;
		let grant_expires_at = now
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), arg.bytes.clone())
			.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;

		let length = match &data {
			tg::object::Data::Blob(blob) => Some(blob.length()),
			_ => None,
		};
		let put = uuid::Uuid::now_v7().into_bytes();

		let put_arg = crate::store::object::put::Arg {
			bytes: Some(arg.bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length,
			put,
		};
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let permission = if self
			.post_object_batch_authorize(
				&arg.children,
				&children,
				&BTreeSet::new(),
				&BTreeSet::new(),
			)
			.await?
		{
			tg::authorization::permission::object::Permission::Subtree
		} else {
			tg::authorization::permission::object::Permission::Node
		};

		let (node_solvable, node_solved) = match data {
			tg::object::Data::File(file) => match file {
				tg::file::Data::Pointer(_) => (false, true),
				tg::file::Data::Node(node) => (node.solvable(), node.solved()),
			},
			tg::object::Data::Graph(graph) => {
				graph
					.nodes
					.iter()
					.fold((false, true), |(solvable, solved), node| {
						if let tg::graph::data::Node::File(file) = node {
							(solvable || file.solvable(), solved && file.solved())
						} else {
							(solvable, solved)
						}
					})
			},
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
		let arg = tangram_index::object::put::Arg {
			checkout: None,
			children,
			id: id.clone(),
			metadata,
			put,
			storage: tangram_index::object::Storage::default(),
			time_to_touch: self.server.config.object.time_to_touch,
			touched_at: now,
		};
		let grant_subject = match &self.context.principal {
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_subject()?),
		};
		let put_grant = grant_subject.map(|grant_subject| tangram_index::grant::put::Arg {
			created_at: now,
			creator: Some(self.context.principal.clone()),
			implicit: Some(Some(grant_expires_at)),
			permissions: tg::authorization::Permission::Object(permission).into(),
			subject: grant_subject,
			resource: id.clone().into(),
			time_to_touch: Some(self.server.config.object.grant_time_to_touch),
		});
		let account = self.usage_account(&self.context.principal).await?;
		let arg = tangram_index::batch::Arg {
			items: std::iter::once(tangram_index::batch::Item::PutObject(arg))
				.chain(put_grant.map(tangram_index::batch::Item::PutGrant))
				.chain(account.map(|account| {
					tangram_index::batch::Item::PutAccountObject(
						tangram_index::usage::storage::put::ObjectArg {
							account,
							object: id.clone(),
							touched_at: now,
						},
					)
				}))
				.collect(),
		};
		self.server.put_object_and_index(put_arg, arg).await?;

		let token = self.create_token(
			id.clone().into(),
			vec![tg::authorization::Permission::Object(permission)],
			grant_expires_at,
		)?;
		let object = tg::Referent::with_node_and_token(id.clone(), token);

		Ok(tg::object::put::Output { object })
	}

	async fn put_object_region(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		region: String,
	) -> tg::Result<tg::object::put::Output> {
		let client = self.get_region_session(&region).await.map_err(
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
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<(tg::object::put::Output, bool)> {
		let client = self.get_remote_session(&remote).await.map_err(
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
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse::<tg::object::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the object id"))?;
		let arg = request
			.query_params::<tg::object::put::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the content type header"))?;
		let body = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the request body"))?;
		let bytes = match content_type
			.as_ref()
			.map(|content_type| (content_type.type_(), content_type.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => {
				let data = serde_json::from_slice::<tg::object::Data>(&body)
					.map_err(|error| tg::error!(!error, "failed to deserialize the request"))?;
				if data.kind() != id.kind() {
					return Err(tg::error!(
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
				return Err(tg::error!(%type_, %subtype, "invalid content type"));
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
			.put_object(&id, arg)
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

impl Server {
	pub(crate) async fn put_object_and_index(
		&self,
		object: crate::store::object::put::Arg,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		let future = self.put_object_and_index_inner(object, index).boxed();
		let result = tokio::time::timeout(self.config.object.put_timeout, future)
			.await
			.map_err(|error| tg::error!(!error, "timed out putting and indexing the object"))?;
		result?;

		Ok(())
	}

	async fn put_object_and_index_inner(
		&self,
		object: crate::store::object::put::Arg,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		if self.config.advanced.single_process {
			self.put_object_inner(object)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the object"))?;
			self.index_batch(index)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the object"))?;

			return Ok(());
		}

		let object_put = self.put_object_inner(object);
		let index_put = self.index_batch(index);
		let (object_result, index_result) = future::join(object_put, index_put).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the object"))?;
		index_result.map_err(|error| tg::error!(!error, "failed to index the object"))?;

		Ok(())
	}

	pub(crate) async fn put_object(&self, arg: crate::store::object::put::Arg) -> tg::Result<()> {
		let future = self.put_object_inner(arg).boxed();
		let result = tokio::time::timeout(self.config.object.put_timeout, future)
			.await
			.map_err(|error| tg::error!(!error, "timed out putting the object"))?;
		result?;

		Ok(())
	}

	async fn put_object_inner(&self, arg: crate::store::object::put::Arg) -> tg::Result<()> {
		if self.archive.is_none() {
			return self
				.store
				.put_object(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the object"));
		}

		let entry = self.object_archive_outbox_entry(arg.id.clone(), arg.put);
		let outbox_arg = crate::store::object::archive::outbox::put::Arg {
			entries: vec![entry.clone()],
		};
		let object_put = self.store.put_object(arg);
		let outbox_put = self.store.put_object_archive_outbox_entries(outbox_arg);
		let (object_result, outbox_result) = future::join(object_put, outbox_put).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the object"))?;
		outbox_result.map_err(|error| {
			tg::error!(
				!error,
				"failed to write an entry to the object archive outbox"
			)
		})?;
		self.spawn_publish_object_archive_outbox_notifications(BTreeSet::from([entry.partition]));

		Ok(())
	}

	pub(crate) async fn put_object_batch_and_index(
		&self,
		objects: Vec<crate::store::object::put::Arg>,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		let future = self
			.put_object_batch_and_index_inner(objects, index)
			.boxed();
		let result = tokio::time::timeout(self.config.object.put_timeout, future)
			.await
			.map_err(|error| {
				tg::error!(!error, "timed out putting and indexing the object batch")
			})?;
		result?;

		Ok(())
	}

	async fn put_object_batch_and_index_inner(
		&self,
		objects: Vec<crate::store::object::put::Arg>,
		index: tangram_index::batch::Arg,
	) -> tg::Result<()> {
		if self.config.advanced.single_process {
			self.put_object_batch_inner(objects)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
			self.index_batch(index)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the object batch"))?;

			return Ok(());
		}

		let object_put = self.put_object_batch_inner(objects);
		let index_put = self.index_batch(index);
		let (object_result, index_result) = future::join(object_put, index_put).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
		index_result.map_err(|error| tg::error!(!error, "failed to index the object batch"))?;

		Ok(())
	}

	pub(crate) async fn put_object_batch(
		&self,
		args: Vec<crate::store::object::put::Arg>,
	) -> tg::Result<()> {
		let future = self.put_object_batch_inner(args).boxed();
		let result = tokio::time::timeout(self.config.object.put_timeout, future)
			.await
			.map_err(|error| tg::error!(!error, "timed out putting the objects"))?;
		result?;

		Ok(())
	}

	async fn put_object_batch_inner(
		&self,
		args: Vec<crate::store::object::put::Arg>,
	) -> tg::Result<()> {
		if self.archive.is_none() {
			return self
				.store
				.put_object_batch(args)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the objects"));
		}

		let entries = args
			.iter()
			.map(|arg| self.object_archive_outbox_entry(arg.id.clone(), arg.put))
			.collect::<Vec<_>>();
		let partitions = entries
			.iter()
			.map(|entry| entry.partition)
			.collect::<BTreeSet<_>>();
		let outbox_arg = crate::store::object::archive::outbox::put::Arg { entries };
		let object_put = self.store.put_object_batch(args);
		let outbox_put = self.store.put_object_archive_outbox_entries(outbox_arg);
		let (object_result, outbox_result) = future::join(object_put, outbox_put).await;
		object_result.map_err(|error| tg::error!(!error, "failed to put the objects"))?;
		outbox_result.map_err(|error| {
			tg::error!(
				!error,
				"failed to write entries to the object archive outbox"
			)
		})?;
		self.spawn_publish_object_archive_outbox_notifications(partitions);

		Ok(())
	}

	fn object_archive_outbox_entry(
		&self,
		id: tg::object::Id,
		put: [u8; 16],
	) -> crate::store::object::archive::outbox::Entry {
		let partition_total = self.config.object.archive_outbox.partition_total;
		let bytes = id.to_bytes();
		let start = bytes.len().saturating_sub(8);
		let mut suffix = [0; 8];
		suffix[8 - (bytes.len() - start)..].copy_from_slice(&bytes[start..]);
		let partition = u64::from_be_bytes(suffix) % partition_total;

		crate::store::object::archive::outbox::Entry { id, partition, put }
	}

	fn spawn_publish_object_archive_outbox_notifications(&self, partitions: BTreeSet<u64>) {
		tokio::spawn({
			let server = self.clone();
			async move {
				for partition in partitions {
					let subject = crate::indexer::object_archive_outbox_subject(partition);
					if let Err(error) = server.messenger.publish(subject, ()).await {
						tracing::error!(%error, %partition, "failed to publish an object archive outbox notification");
					}
				}
			}
		});
	}
}
