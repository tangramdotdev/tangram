use {
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream::FuturesUnordered},
	itertools::Itertools as _,
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		io::{Read, Write as _},
		path::PathBuf,
		pin::pin,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::{
		Session,
		temp::Temp,
		write::{Destination, Output},
	},
	tokio::io::{AsyncRead, AsyncWriteExt as _},
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn write(
		&self,
		session: &Session,
		arg: tg::write::Arg,
		reader: impl AsyncRead,
	) -> tg::Result<tg::write::Output> {
		// Get the timestamps.
		let touched_at = session.server.clock.unix_timestamp();
		let permission_expires_at = touched_at
			+ session
				.server
				.config
				.object
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();

		// Create the destination.
		let destination =
			if session.server.checkouts_enabled() && !session.server.config.advanced.multi_server {
				Destination::Temp(Temp::new(&session.server))
			} else {
				Destination::Cache
			};

		// Create the blob.
		let blob = self
			.write_inner(session, reader, Some(&destination))
			.await
			.map_err(|error| tg::error!(!error, "failed to write the blob"))?;
		let blob = Arc::new(blob);

		// Rename the temp file to the checkouts directory if necessary.
		let checkout_pointers = arg
			.checkout_pointers
			.unwrap_or(session.server.config.write.checkout_pointers);
		let checkout_pointer = if session.server.checkouts_enabled()
			&& let Destination::Temp(temp) = destination
			&& checkout_pointers
		{
			let data = tg::file::Data::Node(tg::file::data::Node {
				contents: Some(blob.id.clone()),
				dependencies: BTreeMap::new(),
				executable: false,
				module: None,
			});
			let id = tg::file::Id::new(&data.serialize()?);
			let path = session.server.checkout_path().join(id.to_string());
			match tangram_util::fs::rename_noreplace(temp.path(), path).await {
				Ok(()) => (),
				Err(error)
					if matches!(
						error.kind(),
						std::io::ErrorKind::AlreadyExists
							| std::io::ErrorKind::IsADirectory
							| std::io::ErrorKind::PermissionDenied
					) => {},
				Err(error) => {
					return Err(tg::error!(
						!error,
						"failed to rename the file to the blobs directory"
					));
				},
			}
			Some((id.into(), None))
		} else {
			None
		};

		// Cache and index the blob.
		let cache_args = Self::write_cache_args(&blob, checkout_pointer.as_ref());
		let index_arg = self
			.write_index_arg(
				session,
				&blob,
				checkout_pointer,
				touched_at,
				permission_expires_at,
			)
			.await?;
		session
			.server
			.components
			.object
			.put_object_batch_and_index(&session.server, cache_args, index_arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to store and index the blob"))?;

		// Create the output.
		let token = session.server.components.authorization.create_token(
			session,
			blob.id.clone().into(),
			vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			permission_expires_at,
		)?;
		let blob = tg::Referent::with_node_and_local_tokens(blob.id.clone(), token);
		let output = tg::write::Output { blob };

		Ok(output)
	}

	pub(crate) async fn write_inner(
		&self,
		session: &Session,
		reader: impl AsyncRead,
		destination: Option<&Destination>,
	) -> tg::Result<Output> {
		self.write_inner_with_instance(reader, destination, 1, |arg| {
			session
				.server
				.components
				.object
				.put_object_with_server(&session.server, arg)
		})
		.await
	}

	async fn write_inner_with_instance<F>(
		&self,
		reader: impl AsyncRead,
		destination: Option<&Destination>,
		concurrency: usize,
		put_object: impl Fn(tangram_server_core::cache::object::put::Arg) -> F,
	) -> tg::Result<Output>
	where
		F: Future<Output = tg::Result<()>>,
	{
		// Create the reader.
		let reader = pin!(reader);
		let mut reader = fastcdc::v2020::AsyncStreamCDC::new(
			reader,
			tg::blob::MIN_LEAF_SIZE,
			tg::blob::AVG_LEAF_SIZE,
			tg::blob::MAX_LEAF_SIZE,
		);
		let stream = reader.as_stream();
		let mut stream = pin!(stream);

		// Open the destination file if necessary.
		let mut file = if let Some(Destination::Temp(temp)) = destination {
			Some(
				tokio::fs::File::create_new(temp.path())
					.await
					.map_err(|error| tg::error!(!error, "failed to create the file"))?,
			)
		} else {
			None
		};

		// Create the leaves and write or store them if necessary.
		let mut blobs = Vec::new();
		let mut puts = FuturesUnordered::new();
		while let Some(chunk) = stream
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to read from the reader"))?
		{
			// Create the leaf.
			let blob = Self::write_inner_leaf(&chunk);

			// Cache the leaf if necessary.
			match destination {
				None => (),
				Some(Destination::Temp(_)) => {
					file.as_mut()
						.unwrap()
						.write_all(&chunk.data)
						.await
						.map_err(|error| tg::error!(!error, "failed to write to the file"))?;
				},
				Some(Destination::Cache) => {
					let mut bytes = vec![0];
					bytes.extend_from_slice(&chunk.data);
					let arg = tangram_server_core::cache::object::put::Arg {
						bytes: Some(bytes.into()),
						checkout_pointer: None,
						id: blob.id.clone().into(),
						length: Some(blob.length),
						put: blob.put,
					};
					puts.push(put_object(arg));
					if puts.len() >= concurrency {
						puts.try_next()
							.await
							.map_err(|error| tg::error!(!error, "failed to store the leaf"))?;
					}
				},
			}

			blobs.push(blob);
		}
		while puts
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to store a leaf"))?
			.is_some()
		{}

		// Flush and close the file if necessary.
		if let Some(mut file) = file {
			file.flush()
				.await
				.map_err(|error| tg::error!(!error, "failed to flush the file"))?;
			drop(file);
		}

		// Create the tree.
		let max_branch_children = tg::blob::MAX_BRANCH_CHILDREN;
		while blobs.len() > max_branch_children {
			blobs = blobs
				.chunks(max_branch_children)
				.flat_map(|chunk| {
					if chunk.len() == max_branch_children {
						vec![Self::write_inner_branch(chunk.to_vec())]
					} else {
						chunk.iter().cloned().map(Ok).collect()
					}
				})
				.collect::<tg::Result<Vec<_>>>()?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => Self::write_inner_empty_leaf(),
			1 => blobs[0].clone(),
			_ => Self::write_inner_branch(blobs)?,
		};

		Ok(blob)
	}

	pub fn write_inner_sync(
		&self,
		session: &Session,
		mut reader: impl Read,
		destination: Option<&Destination>,
		progress: &tg::progress::Handle<tangram_server_core::checkin::TaskOutput>,
	) -> tg::Result<Output> {
		// Create the chunker.
		let mut chunker = fastcdc::v2020::StreamCDC::new(
			&mut reader,
			tg::blob::MIN_LEAF_SIZE,
			tg::blob::AVG_LEAF_SIZE,
			tg::blob::MAX_LEAF_SIZE,
		);

		// Open the destination file if necessary.
		let mut file = if let Some(Destination::Temp(temp)) = destination {
			Some(
				std::fs::File::create_new(temp.path())
					.map_err(|error| tg::error!(!error, "failed to create the file"))?,
			)
		} else {
			None
		};

		// Create the leaves and write or store them if necessary.
		let mut blobs = Vec::new();
		for chunk in &mut chunker {
			let chunk =
				chunk.map_err(|error| tg::error!(!error, "failed to read from the reader"))?;

			// Create the leaf.
			let blob = Self::write_inner_leaf(&chunk);

			// Cache the leaf if necessary.
			match destination {
				None => (),
				Some(Destination::Temp(_)) => {
					file.as_mut()
						.unwrap()
						.write_all(&chunk.data)
						.map_err(|error| tg::error!(!error, "failed to write to the file"))?;
				},
				Some(Destination::Cache) => {
					let mut bytes = vec![0];
					bytes.extend_from_slice(&chunk.data);
					let arg = tangram_server_core::cache::object::put::Arg {
						bytes: Some(bytes.into()),
						checkout_pointer: None,
						id: blob.id.clone().into(),
						length: Some(blob.length),
						put: blob.put,
					};
					session
						.server
						.cache
						.put_object_sync(arg)
						.map_err(|error| tg::error!(!error, "failed to store the leaf"))?;
				},
			}
			progress.increment("bytes", chunk.length.to_u64().unwrap());
			blobs.push(blob);
		}

		// Flush and close the file if necessary.
		if let Some(mut file) = file {
			file.flush()
				.map_err(|error| tg::error!(!error, "failed to flush the file"))?;
			drop(file);
		}

		// Create the tree.
		let max_branch_children = tg::blob::MAX_BRANCH_CHILDREN;
		while blobs.len() > max_branch_children {
			blobs = blobs
				.chunks(max_branch_children)
				.flat_map(|chunk| {
					if chunk.len() == max_branch_children {
						vec![Self::write_inner_branch(chunk.to_vec())]
					} else {
						chunk.iter().cloned().map(Ok).collect()
					}
				})
				.collect::<tg::Result<Vec<_>>>()?;
		}

		// Get the blob.
		let blob = match blobs.len() {
			0 => Self::write_inner_empty_leaf(),
			1 => blobs[0].clone(),
			_ => Self::write_inner_branch(blobs)?,
		};

		Ok(blob)
	}

	fn write_inner_empty_leaf() -> Output {
		let bytes = vec![0];
		let id = tg::blob::Id::new(&bytes);
		Output {
			bytes: Some(bytes.into()),
			children: Vec::new(),
			data: None,
			id,
			length: 0,
			metadata: tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: 1,
					solvable: false,
					solved: true,
				},
				subtree: tg::object::metadata::Subtree {
					count: Some(1),
					depth: Some(1),
					size: Some(1),
					solvable: Some(false),
					solved: Some(true),
				},
			},
			position: 0,
			put: uuid::Uuid::now_v7().into_bytes(),
		}
	}

	fn write_inner_leaf(chunk: &fastcdc::v2020::ChunkData) -> Output {
		let fastcdc::v2020::ChunkData {
			offset: position,
			length,
			data,
			..
		} = chunk;
		let length = length.to_u64().unwrap();
		let size = 1 + length;
		let mut data_ = vec![0];
		data_.extend_from_slice(data);
		let id = tg::blob::Id::new(&data_);
		Output {
			bytes: None,
			children: Vec::new(),
			data: None,
			id,
			length,
			metadata: tg::object::Metadata {
				node: tg::object::metadata::Node {
					size,
					solvable: false,
					solved: true,
				},
				subtree: tg::object::metadata::Subtree {
					count: Some(1),
					depth: Some(1),
					size: Some(size),
					solvable: Some(false),
					solved: Some(true),
				},
			},
			position: *position,
			put: uuid::Uuid::now_v7().into_bytes(),
		}
	}

	fn write_inner_branch(children: Vec<Output>) -> tg::Result<Output> {
		let children_ = children
			.iter()
			.map(|child| tg::blob::data::Child {
				blob: child.id.clone(),
				length: child.length,
			})
			.collect();
		let data = tg::blob::Data::Branch(tg::blob::data::Branch {
			children: children_,
		});
		let bytes = data.serialize()?;
		let size = bytes.len().to_u64().unwrap();
		let id = tg::blob::Id::new(&bytes);
		let entry = tg::object::Metadata {
			node: tg::object::metadata::Node {
				size,
				solvable: false,
				solved: true,
			},
			subtree: tg::object::metadata::Subtree {
				count: Some(1),
				depth: Some(1),
				size: Some(size),
				solvable: Some(false),
				solved: Some(true),
			},
		};
		let metadata =
			children
				.iter()
				.unique_by(|blob| &blob.id)
				.fold(entry, |mut metadata, child| {
					let child_subtree = &child.metadata.subtree;
					metadata.subtree.count = metadata
						.subtree
						.count
						.zip(child_subtree.count)
						.map(|(a, b)| a + b);
					metadata.subtree.depth = metadata
						.subtree
						.depth
						.zip(child_subtree.depth)
						.map(|(a, b)| a.max(1 + b));
					metadata.subtree.size = metadata
						.subtree
						.size
						.zip(child_subtree.size)
						.map(|(a, b)| a + b);
					metadata
				});
		let position = children.first().unwrap().position;
		let length = children.iter().map(|child| child.length).sum();
		let output = Output {
			bytes: Some(bytes),
			children,
			data: Some(data),
			id,
			length,
			metadata,
			position,
			put: uuid::Uuid::now_v7().into_bytes(),
		};
		Ok(output)
	}

	pub(crate) fn write_cache_args(
		blob: &Output,
		checkout_pointer: Option<&(tg::artifact::Id, Option<PathBuf>)>,
	) -> Vec<tangram_server_core::cache::object::put::Arg> {
		let mut args = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			if blob.bytes.is_none() && checkout_pointer.is_none() {
				stack.extend(&blob.children);
				continue;
			}
			let checkout_pointer = checkout_pointer.as_ref().map(|(artifact, path)| {
				tangram_server_core::cache::object::checkout::Pointer {
					artifact: artifact.clone(),
					length: blob.length,
					path: path.clone(),
					position: blob.position,
				}
			});
			args.push(tangram_server_core::cache::object::put::Arg {
				bytes: blob.bytes.clone(),
				checkout_pointer,
				id: blob.id.clone().into(),
				length: Some(blob.length),
				put: blob.put,
			});
			stack.extend(&blob.children);
		}
		args
	}

	async fn write_index_arg(
		&self,
		session: &Session,
		blob: &Output,
		checkout_pointer: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
		permission_expires_at: i64,
	) -> tg::Result<tangram_index::batch::Arg> {
		// Create the index args.
		let (put_checkout_args, mut put_object_args, put_permission_args) = Self::write_index_args(
			blob,
			checkout_pointer,
			touched_at,
			(!matches!(
				session.context.principal,
				tg::Principal::Anonymous | tg::Principal::Root
			))
			.then_some(&session.context.principal),
			permission_expires_at,
			session.server.config.object.permission_time_to_touch,
			session.server.config.object.time_to_touch,
		);

		// Get the usage account.
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &session.context.principal)
			.await?;

		// Put the account's storage entry with the root object.
		let root_object = tg::object::Id::from(blob.id.clone());
		if let Some(arg) = put_object_args.iter_mut().find(|arg| arg.id == root_object) {
			arg.account = account;
		}

		// Create the batch.
		let arg = tangram_index::batch::Arg {
			items: put_checkout_args
				.into_iter()
				.map(tangram_index::batch::Item::PutCheckout)
				.chain(
					put_object_args
						.into_iter()
						.map(tangram_index::batch::Item::PutObject),
				)
				.chain(
					put_permission_args
						.into_iter()
						.map(tangram_index::batch::Item::PutPermission),
				)
				.collect(),
		};

		Ok(arg)
	}

	pub(crate) fn write_index_args(
		blob: &Output,
		checkout_pointer: Option<(tg::artifact::Id, Option<PathBuf>)>,
		touched_at: i64,
		principal: Option<&tg::Principal>,
		permission_expires_at: i64,
		permission_time_to_touch: std::time::Duration,
		time_to_touch: std::time::Duration,
	) -> (
		Vec<tangram_index::checkout::put::Arg>,
		Vec<tangram_index::object::put::Arg>,
		Vec<tangram_index::permission::put::Arg>,
	) {
		// Collect the blobs in topological order.
		let mut blobs = Vec::new();
		let mut stack = vec![blob];
		while let Some(blob) = stack.pop() {
			blobs.push(blob);
			stack.extend(&blob.children);
		}

		// Create put object args in reverse topological order.
		let mut put_object_args = Vec::with_capacity(blobs.len());
		for blob in blobs.into_iter().rev() {
			let checkout = checkout_pointer
				.as_ref()
				.map(|(artifact, _)| artifact.clone());
			let mut children = BTreeSet::new();
			if let Some(data) = &blob.data {
				data.children(&mut children);
			}
			let id = blob.id.clone().into();
			let arg = tangram_index::object::put::Arg {
				account: None,
				checkout,
				children,
				id,
				metadata: blob.metadata.clone(),
				put: blob.put,
				storage: tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE,
				time_to_touch,
				touched_at,
			};
			put_object_args.push(arg);
		}

		// Create a checkout arg if necessary.
		let put_checkout_args = if let Some((artifact, _)) = checkout_pointer {
			vec![tangram_index::checkout::put::Arg {
				dependencies: Vec::new(),
				id: artifact.into(),
				replace: false,
				touched_at,
			}]
		} else {
			vec![]
		};

		// Grant the subject subtree read access to the blob, since it produced the entire blob.
		let put_permission_args = principal
			.map(|principal| tangram_index::permission::put::Arg {
				created_at: touched_at,
				creator: Some(principal.clone()),
				permissions: tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)
				.into(),
				resource: tg::object::Id::from(blob.id.clone()).into(),
				source: tangram_index::permission::Source::Direct {
					expires_at: Some(permission_expires_at),
				},
				subject: principal
					.try_to_subject()
					.expect("expected the principal to be a valid authorization subject"),
				time_to_touch: Some(permission_time_to_touch),
				version: None,
			})
			.into_iter()
			.collect();

		(put_checkout_args, put_object_args, put_permission_args)
	}

	pub(crate) async fn write_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let (arg, request) = request
			.arg::<tg::write::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Write.
		let output = self
			.write(session, arg, request.reader())
			.await
			.map_err(|error| tg::error!(!error, "failed to write"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
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

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["write"]) => self.write_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::write::Component for Component {
	fn write_inner_sync<'a>(
		&'a self,
		session: &'a Session,
		reader: &'a mut dyn std::io::Read,
		destination: Option<&'a tangram_server_core::write::Destination>,
		progress: &'a tg::progress::Handle<tangram_server_core::checkin::TaskOutput>,
	) -> tg::Result<tangram_server_core::write::Output> {
		self.write_inner_sync(session, reader, destination, progress)
	}

	fn write<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::write::Arg,
		reader: std::pin::Pin<Box<dyn AsyncRead + Send + 'static>>,
	) -> BoxFuture<'a, tg::Result<tg::write::Output>> {
		self.write(session, arg, reader).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
