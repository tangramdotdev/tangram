use {crate::Session, tangram_client::prelude::*};

impl Session {
	pub(super) async fn finish_process_control_request(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::control::FinishClientRequestArg,
		sync: Option<&tg::sync::Token>,
	) -> tg::Result<tg::process::control::FinishServerResponseOutput> {
		crate::checkpoint!(self.server, "process.control.finish", id = %id).await;

		// Associate the output with the incoming sync before publishing the finished process.
		if let Some(sync) = sync {
			let location = tg::Location::Local(tg::location::Local::default());
			if let Some(data) = arg.data.output.take() {
				let value = tg::Value::try_from_data(data)?;
				for object in value.objects() {
					let mut tokens = object.state().tokens();
					tokens.insert_sync(location.clone(), sync.clone());
					object.state().set_tokens(tokens);
				}
				arg.data.output = Some(value.to_data());
			}
			if let Some(tg::Either::Right(error)) = &mut arg.data.error {
				error.options.tokens.insert_sync(location, sync.clone());
			}
		}

		let options = crate::process::put::Options {
			defer_index: false,
			enqueue_log_compaction: false,
			location: None,
			store_data: true,
		};
		self.put_finished_process_local(id, arg.data, options)
			.await?;
		self.spawn_process_finish_tasks(id);

		Ok(tg::process::control::FinishServerResponseOutput {})
	}

	pub(crate) async fn store_process_error(
		&self,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Either<tg::error::Data, tg::error::Id> {
		let tg::Either::Left(mut data) = error else {
			return error;
		};
		if !self.server.config.advanced.internal_error_locations {
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
		let result = error.store_with_handle(self).await;
		match result {
			Ok(id) => tg::Either::Right(id),
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to store the process error");
				tg::Either::Left(data)
			},
		}
	}

	pub(crate) fn spawn_process_finish_tasks(&self, id: &tg::process::Id) {
		// Spawn a task to publish the stdin close message.
		self.server
			.spawn_publish_process_stdio_close_message_task(id, tg::process::stdio::Stream::Stdin);

		// Spawn a task to publish the status.
		self.server.spawn_publish_process_status_task(id);
	}
}
