use {
	crate::Session,
	futures::{Stream, StreamExt as _, future, stream},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + use<>,
	> {
		if arg
			.nodes
			.iter()
			.all(|node| node.node.kind() == tg::id::Kind::Process || node.node.kind().is_object())
			&& self
				.pull_nodes_available_local(&arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to check whether the pull is local"))?
		{
			let stream = stream::once(future::ok(tg::progress::Event::Output(
				tg::pull::Output::default(),
			)));
			return Ok(stream.boxed());
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
		let stream = self
			.push_or_pull(&arg, Vec::new(), source, destination)
			.await?;
		Ok(stream.boxed())
	}

	async fn pull_nodes_available_local(&self, arg: &tg::pull::Arg) -> tg::Result<bool> {
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
		let objects_stored = objects
			.into_iter()
			.all(|object| object.is_some_and(|object| object.storage.subtree));
		let processes_stored = processes.into_iter().all(|process| {
			let Some(process) = process else {
				return false;
			};
			if process.data.is_none() {
				return false;
			}
			let storage = process.storage;
			if arg.process_children {
				storage.subtree
					&& (!arg.process_commands || storage.subtree_command)
					&& (!arg.process_errors || storage.subtree_error)
					&& (!arg.process_logs || storage.subtree_log)
					&& (!arg.process_outputs || storage.subtree_output)
			} else {
				(!arg.process_commands || storage.node_command)
					&& (!arg.process_errors || storage.node_error)
					&& (!arg.process_logs || storage.node_log)
					&& (!arg.process_outputs || storage.node_output)
			}
		});
		let stored = objects_stored && processes_stored;
		if !stored {
			return Ok(false);
		}

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
		let available = outputs.into_iter().zip(required).all(|(output, required)| {
			output.is_some_and(|permissions| permissions.contains(required))
		});

		Ok(available)
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
				arg.process_commands,
				tg::authorization::permission::process::Permission::NodeCommand,
				tg::authorization::permission::process::Permission::SubtreeCommand,
			),
			(
				arg.process_errors,
				tg::authorization::permission::process::Permission::NodeError,
				tg::authorization::permission::process::Permission::SubtreeError,
			),
			(
				arg.process_logs,
				tg::authorization::permission::process::Permission::NodeLog,
				tg::authorization::permission::process::Permission::SubtreeLog,
			),
			(
				arg.process_outputs,
				tg::authorization::permission::process::Permission::NodeOutput,
				tg::authorization::permission::process::Permission::SubtreeOutput,
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
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
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
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
