use {
	crate::Component,
	futures::FutureExt as _,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::{Server, Session},
};

impl Component {
	/// Create the queued record of a process, which is visible until its runner connects, with the permissions of its creator and the public permission of a public process if requested. The boot is the boot of the sandbox that the process belongs to, if its holder knows it.
	pub(crate) fn create_queued_process_arg(
		session: &Session,
		process: &tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
		boot: Option<u64>,
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
			boot,
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
			sandbox_boot: None,
			sequence: 0,
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
		self.finish_queued_process(server, &id, process.data.clone(), error, None, None)
			.await?;

		Ok(())
	}

	/// Finish the queued processes of a sandbox whose boot ended, which can never start, with the sandbox's error, or with a cancellation if the sandbox ended without one. Each finish is a conditional index write that applies only while the sandbox is at the boot that ended, so that a process queued for a later boot is never finished. A process that another finisher, such as its spawner, finished first keeps that record, because the index rejects a second terminal record that differs.
	pub(crate) async fn finish_queued_sandbox_processes(
		&self,
		server: &Server,
		sandbox: &tg::sandbox::Id,
		boot: u64,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::Result<()> {
		// Skip the reads if the index already records a later boot, whose conditional finishes would all be rejected.
		let later = server
			.index
			.try_get_sandbox(sandbox)
			.await
			.map_err(|source| tg::error!(!source, %sandbox, "failed to get the sandbox"))?
			.and_then(|indexed| indexed.data)
			.is_some_and(|data| data.data.boot > boot);
		if later {
			return Ok(());
		}

		// Get the sandbox's processes.
		let length = server
			.index
			.try_get_sandbox_processes_count(sandbox)
			.await?
			.unwrap_or_default();
		let processes = server
			.index
			.get_sandbox_processes(sandbox, std::io::SeekFrom::Start(0), length)
			.await
			.map_err(
				|source| tg::error!(!source, %sandbox, "failed to get the sandbox processes"),
			)?;
		let indexed = server
			.index
			.try_get_processes_with_data(&processes)
			.await
			.map_err(|source| tg::error!(!source, %sandbox, "failed to get the process records"))?;

		// Finish each queued process.
		let error = error.unwrap_or_else(|| {
			tg::Either::Left(tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the sandbox ended".to_owned()),
				..Default::default()
			})
		});
		for (process, indexed) in std::iter::zip(processes, indexed) {
			let Some((_, Some(data))) = indexed else {
				continue;
			};
			if !data.status.is_queued() {
				continue;
			}
			let sandbox_boot = tangram_index::process::put::SandboxBoot {
				boot,
				sandbox: sandbox.clone(),
			};
			if let Err(error) = self
				.finish_queued_process(
					server,
					&process,
					data,
					error.clone(),
					Some(sandbox_boot),
					None,
				)
				.await
			{
				tracing::debug!(error = %error.trace(), %process, "failed to finish the queued process");
			}
		}

		Ok(())
	}

	/// Record the failure of a queued process that will never start, only while its sandbox is at the boot that ended if one is given, and with the registration that must still own the runner if one is given. Run the follow-up work only if the index applies the record.
	pub(crate) async fn finish_queued_process(
		&self,
		server: &Server,
		id: &tg::process::Id,
		mut data: tg::process::Data,
		error: tg::Either<tg::error::Data, tg::error::Id>,
		sandbox_boot: Option<tangram_index::process::put::SandboxBoot>,
		registration: Option<tangram_index::runner::Registration>,
	) -> tg::Result<()> {
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
			registration,
			sandbox_boot,
			store_data: true,
		};
		self.put_process_local(&session, id, entry, options)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, process = %id, "failed to store the process"))?;
		let session = server.session(&server.context);
		self.spawn_process_finish_tasks(&session, id);

		Ok(())
	}
}
