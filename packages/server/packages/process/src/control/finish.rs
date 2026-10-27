use {
	crate::Component, std::hash::BuildHasher as _, tangram_client::prelude::*,
	tangram_server_core::Session,
};

impl Component {
	pub(super) async fn finish_process_control_request(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::control::FinishClientRequestArg,
		command_sync: Option<tg::Referent<tg::sync::Id>>,
		outcome_sync: Option<tg::Referent<tg::sync::Id>>,
	) -> tg::Result<tg::process::control::FinishServerResponseOutput> {
		Self::spawn_publish_process_status_task(&session.server, id);
		tangram_server_core::checkpoint!(session.server, "process.control.finish", id = %id).await;
		let data = arg.data.clone();

		// Record the location with the finished write, so that it does not depend on the started write being indexed first.
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let options = tangram_server_process_core::put::Options {
			command_sync,
			defer_index: true,
			location: Some(location),
			outcome_sync,
			store_data: true,
		};
		self.put_finished_process_local(session, id, arg.data, options)
			.await?;
		self.expire_process_log_cache(session, id, &data).await?;

		Self::spawn_publish_process_stdio_close_message_task(
			&session.server,
			id,
			tg::process::stdio::Stream::Stdin,
		);
		tangram_server_core::checkpoint!(session.server, "process.control.finish.submitted", process = %id).await;

		Ok(tg::process::control::FinishServerResponseOutput {})
	}

	pub async fn expire_process_log_cache(
		&self,
		session: &Session,
		id: &tg::process::Id,
		data: &tg::process::Data,
	) -> tg::Result<()> {
		if !data.stdout.is_log() && !data.stderr.is_log() {
			return Ok(());
		}
		let finished_at = data
			.finished_at
			.ok_or_else(|| tg::error!("missing the process finish timestamp"))?;
		let expires_at = finished_at
			+ i64::try_from(session.server.config.process.log_time_to_live.as_secs()).unwrap();
		let partition = tg::id::BuildHasher.hash_one(id)
			% session.server.config.process.log_cache_partition_total;
		let entry = tangram_cache::log::cache::Entry {
			expires_at,
			partition,
			process: id.clone(),
		};
		let arg = tangram_cache::log::cache::put::Arg { entry };
		session
			.server
			.cache
			.put_log_cache_entry(arg)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to register the log cache expiration"),
			)?;
		Ok(())
	}

	pub(crate) fn inherit_process_command_authorization_tokens_for_sync(
		data: &mut tg::process::Data,
		sync: &tg::Referent<tg::sync::Id>,
	) {
		let location = tg::Location::Local(tg::location::Local::default());
		let authorization_tokens = sync.options.tokens.for_location(&location);
		data.command.options.tokens.inherit(&authorization_tokens);
	}

	pub(crate) fn inherit_process_authorization_tokens_for_sync(
		data: &mut tg::process::Data,
		sync: &tg::Referent<tg::sync::Id>,
	) {
		let location = tg::Location::Local(tg::location::Local::default());
		let authorization_tokens = sync.options.tokens.for_location(&location);
		if let Some(error) = &mut data.error {
			error.options.tokens.inherit(&authorization_tokens);
		}
		if let Some(log) = &mut data.log {
			log.options.tokens.inherit(&authorization_tokens);
		}
		if let Some(output) = &mut data.output {
			output.options.tokens.inherit(&authorization_tokens);
		}
	}

	pub(crate) async fn store_process_error(
		&self,
		session: &Session,
		error: tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>,
	) -> tg::Either<tg::error::Data, tg::Referent<tg::error::Id>> {
		let tg::Either::Left(mut data) = error else {
			return error;
		};
		if !session.server.config.advanced.internal_error_locations {
			data.remove_internal_locations();
		}

		let object = match tg::error::Object::try_from_data(data.clone()) {
			Ok(object) => object,
			Err(error) => {
				let error = tg::error!(!error, "failed to create the error object");
				tracing::error!(error = %error.trace(), "failed to store the process error");
				return tg::Either::Left(data);
			},
		};

		let error = tg::Error::with_object(object);
		let result = error.store_with_instance(session).await;
		match result {
			Ok(_) => tg::Either::Right(error.to_referent()),
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to store the process error");
				tg::Either::Left(data)
			},
		}
	}

	pub fn spawn_process_finish_tasks(&self, session: &Session, id: &tg::process::Id) {
		// Spawn a task to publish the stdin close message.
		Self::spawn_publish_process_stdio_close_message_task(
			&session.server,
			id,
			tg::process::stdio::Stream::Stdin,
		);

		// Spawn a task to publish the status.
		Self::spawn_publish_process_status_task(&session.server, id);
	}
}
