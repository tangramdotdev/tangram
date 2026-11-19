use {
	crate::Component,
	futures::FutureExt as _,
	tangram_client::prelude::*,
	tangram_server_core::{Server, Session},
};

impl Component {
	/// Create the queued record of a process, which is visible until its runner connects, with the permissions of its creator and the public permission of a public process if requested.
	pub(crate) fn create_queued_process_arg(
		session: &Session,
		process: &tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
		now: i64,
		permissions: bool,
		public: bool,
	) -> tg::Result<tangram_index::process::put::Arg> {
		let id = process
			.id
			.clone()
			.ok_or_else(|| tg::error!("expected the queued process to have an id"))?;

		// Create the creator permission and the command captures from the data with its tokens, so that the creator can access the process as soon as it is visible.
		let (captures, permissions) = if permissions {
			Self::create_queued_process_permissions(session, &id, &process.data, public)?
		} else {
			(
				tangram_index::permission::capture::Captures::default(),
				Vec::new(),
			)
		};

		// Create the record.
		let data = process
			.data
			.clone()
			.without_location_and_tokens_except_sync();
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let arg = tangram_index::process::put::Arg {
			account: None,
			cached: false,
			captures,
			children: None,
			command: Some(
				data.command
					.objects()
					.into_iter()
					.map(|object| object.node)
					.collect(),
			),
			command_id: data.command.command_id()?.into(),
			data: Some(data),
			error: None,
			id: id.clone(),
			location: Some(location),
			log: None,
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: None,
			parent: None,
			permissions,
			principal: tg::Principal::Process(id),
			registration: None,
			sandbox: Some(sandbox.clone()),
			storage: tg::process::storage::Set::NODE,
			time_to_touch: session.server.config.process.time_to_touch,
			touched_at: now,
		};

		Ok(arg)
	}

	/// Create the permission that lets the creator of a queued process access it, the temporary public permission of a public process, and the captures that give the process the creator's access to its command.
	fn create_queued_process_permissions(
		session: &Session,
		id: &tg::process::Id,
		data: &tg::process::Data,
		public: bool,
	) -> tg::Result<(
		tangram_index::permission::capture::Captures,
		Vec<tangram_index::permission::put::Arg>,
	)> {
		let config = &session.server.config.process;
		let mut permissions = tangram_server_process_core::permission::creator_permission_arg(
			&data.creator,
			id,
			data.created_at,
			config.permission_time_to_live,
			config.permission_time_to_touch,
		)?
		.into_iter()
		.collect::<Vec<_>>();
		if public {
			let permission = tangram_server_process_core::permission::public_permission_arg(
				id,
				data.created_at,
				config.permission_time_to_live,
				config.permission_time_to_touch,
			)?;
			permissions.push(permission);
		}
		let roots = data
			.command
			.objects()
			.into_iter()
			.map(|root| root.map(Into::into))
			.collect();
		let captures = session.server.components.authorization.create_captures(
			session,
			id.clone().into(),
			None,
			roots,
			data.creator.clone(),
			data.created_at,
		)?;
		let output = (captures, permissions);

		Ok(output)
	}

	/// Record the failure of a process that will not start, because no runner will report it.
	pub(crate) async fn finish_unstarted_process(
		&self,
		server: &Server,
		process: &tg::runner::control::Process,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Result<()> {
		let Some(id) = process.id.clone() else {
			return Ok(());
		};
		let mut data = process.data.clone();
		data.cacheable = false;
		data.children.get_or_insert_default();
		data.exit = Some(1);
		data.finished_at = Some(server.clock.unix_timestamp());
		data.set_error(Some(error.map_right(tg::Referent::with_node)));
		data.started_at = None;
		data.status = tg::process::Status::Finished;
		let mut context = server.context.clone();
		context.principal = tg::Principal::Process(id.clone());
		let session = server.session(&context);
		let entry = tg::process::put::Arg {
			data,
			location: None,
			public: false,
		};
		let options = tangram_server_process_core::put::Options {
			command_sync: None,
			defer_index: false,
			location: None,
			outcome_sync: None,
			registration: None,
			store_data: true,
		};
		self.put_process_local(&session, &id, entry, options)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, process = %id, "failed to store the process"))?;
		let session = server.session(&server.context);
		self.spawn_process_finish_tasks(&session, &id);

		Ok(())
	}
}
