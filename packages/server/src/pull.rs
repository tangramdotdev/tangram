use {
	crate::Session,
	futures::{Stream, StreamExt as _, future, stream},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<(
		tg::pull::Header,
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + use<>,
	)> {
		if let Some(nodes) = self
			.try_pull_nodes_local(&arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to pull the nodes locally"))?
		{
			let output = tg::pull::Output {
				nodes,
				..Default::default()
			};
			let stream = stream::once(future::ok(tg::progress::Event::Output(output)));
			return Ok((tg::pull::Header::default(), stream.boxed()));
		}

		let source = arg.source.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let destination = arg
			.destination
			.clone()
			.unwrap_or_else(|| tg::Location::Local(tg::location::Local::default()));
		let arg: tg::push::Arg = arg.clone().into();
		let (header, stream) = if matches!(
			self.context.principal,
			tg::Principal::Process(_) | tg::Principal::Sandbox(_)
		) {
			self.push_or_pull_for_process(&arg, source, destination, None)
				.await?
		} else {
			self.push_or_pull(&arg, source, destination).await?
		};
		Ok((header, stream.boxed()))
	}

	async fn try_pull_nodes_local(
		&self,
		arg: &tg::pull::Arg,
	) -> tg::Result<Option<Vec<tg::Referent<tg::Id>>>> {
		if !arg
			.nodes
			.iter()
			.all(|node| node.node.kind() == tg::id::Kind::Process || node.node.kind().is_object())
		{
			return Ok(None);
		}

		// Check the local storage.
		let touched_at = self.server.clock.unix_timestamp()?;
		let object_ids = arg
			.nodes
			.iter()
			.filter_map(|node| tg::object::Id::try_from(node.node.clone()).ok())
			.collect::<Vec<_>>();
		let process_ids = arg
			.nodes
			.iter()
			.filter_map(|node| tg::process::Id::try_from(node.node.clone()).ok())
			.collect::<Vec<_>>();
		let account = self.usage_account(&self.context.principal).await?;
		let touch_objects_future = async {
			self.server
				.index
				.touch_objects_with_account(
					&object_ids,
					account.as_ref(),
					touched_at,
					self.server.config.object.time_to_touch,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to touch the objects"))
		};
		let touch_processes_future = async {
			self.server
				.index
				.touch_processes_with_account(
					&process_ids,
					account.as_ref(),
					touched_at,
					self.server.config.process.time_to_touch,
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to touch the processes"))
		};
		let (objects, processes) =
			futures::try_join!(touch_objects_future, touch_processes_future)?;
		let objects_stored = objects.into_iter().all(|object| {
			object.is_some_and(|object| object.storage.contains(tg::object::storage::Set::SUBTREE))
		});
		let processes_stored = processes.into_iter().all(|process| {
			let Some(process) = process else {
				return false;
			};
			if process.data.is_none() {
				return false;
			}
			let storage = process.storage;
			if arg.process_children {
				storage.contains(tg::process::storage::Set::SUBTREE)
					&& (!arg.process_command_objects
						|| storage.contains(tg::process::storage::Set::SUBTREE_COMMAND_OBJECTS))
					&& (!arg.process_error_objects
						|| storage.contains(tg::process::storage::Set::SUBTREE_ERROR_OBJECTS))
					&& (!arg.process_log_objects
						|| storage.contains(tg::process::storage::Set::SUBTREE_LOG_OBJECTS))
					&& (!arg.process_output_objects
						|| storage.contains(tg::process::storage::Set::SUBTREE_OUTPUT_OBJECTS))
			} else {
				(!arg.process_command_objects
					|| storage.contains(tg::process::storage::Set::NODE_COMMAND_OBJECTS))
					&& (!arg.process_error_objects
						|| storage.contains(tg::process::storage::Set::NODE_ERROR_OBJECTS))
					&& (!arg.process_log_objects
						|| storage.contains(tg::process::storage::Set::NODE_LOG_OBJECTS))
					&& (!arg.process_output_objects
						|| storage.contains(tg::process::storage::Set::NODE_OUTPUT_OBJECTS))
			}
		});
		let stored = objects_stored && processes_stored;
		if !stored {
			return Ok(None);
		}

		// Authorize the requested permissions.
		let args = arg
			.nodes
			.iter()
			.cloned()
			.map(|node| {
				let permissions = Self::pull_node_permissions(arg, &node.node);
				(node, permissions)
			})
			.collect::<Vec<_>>();
		let required = args
			.iter()
			.map(|(_, permissions)| *permissions)
			.collect::<Vec<_>>();
		let outputs = self.authorize_batch(args).await?;
		if !outputs
			.iter()
			.zip(&required)
			.all(|(output, required)| output.permissions.contains(*required))
		{
			return Ok(None);
		}

		// Sign tokens for the verified permissions, bounded by the authorization proofs.
		let created_at = self.server.clock.unix_timestamp()?;
		let mut nodes = Vec::with_capacity(arg.nodes.len());
		for ((node, authorization), permissions) in arg.nodes.iter().zip(outputs).zip(required) {
			let time_to_live = if node.node.kind().is_object() {
				self.server.config.object.permission_time_to_live
			} else {
				self.server.config.process.permission_time_to_live
			};
			let time_to_live = i64::try_from(time_to_live.as_secs()).map_err(|error| {
				tg::error!(!error, "failed to convert the permission time to live")
			})?;
			let expires_at = created_at
				.checked_add(time_to_live)
				.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
			let expires_at = authorization
				.expires_at
				.map_or(expires_at, |expiration| expiration.min(expires_at));
			let id = node.node.clone();
			let token = self.create_token(id.clone(), permissions.iter().collect(), expires_at)?;
			let node = tg::Referent::with_node_and_local_tokens(id, token);
			nodes.push(node);
		}

		Ok(Some(nodes))
	}

	fn pull_node_permissions(
		arg: &tg::pull::Arg,
		id: &tg::Id,
	) -> tg::authorization::permission::Set {
		if id.kind().is_object() {
			let permission = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			);
			let permissions = tg::authorization::permission::Set::from_permission(permission);

			return permissions;
		}
		debug_assert_eq!(id.kind(), tg::id::Kind::Process);

		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let mut permissions = tg::authorization::permission::Set::from_permission(permission);
		let mut insert = |permission| {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				tg::authorization::Permission::Process(permission),
			));
		};
		if arg.process_children {
			insert(tg::authorization::permission::process::Permission::Subtree);
		}
		for (enabled, node, subtree) in [
			(
				arg.process_command_objects,
				tg::authorization::permission::process::Permission::NodeCommandObjects,
				tg::authorization::permission::process::Permission::SubtreeCommandObjects,
			),
			(
				arg.process_error_objects,
				tg::authorization::permission::process::Permission::NodeErrorObjects,
				tg::authorization::permission::process::Permission::SubtreeErrorObjects,
			),
			(
				arg.process_log_objects,
				tg::authorization::permission::process::Permission::NodeLogObjects,
				tg::authorization::permission::process::Permission::SubtreeLogObjects,
			),
			(
				arg.process_output_objects,
				tg::authorization::permission::process::Permission::NodeOutputObjects,
				tg::authorization::permission::process::Permission::SubtreeOutputObjects,
			),
		] {
			if enabled {
				insert(node);
				if arg.process_children {
					insert(subtree);
				}
			}
		}

		permissions
	}

	pub(crate) async fn pull_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the header and stream.
		let (header, stream) = self
			.pull(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the pull"))?;

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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};

		let body = tangram_http::body::header::set(body, &header)
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
