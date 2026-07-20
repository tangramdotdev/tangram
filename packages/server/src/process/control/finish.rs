use {crate::Session, tangram_client::prelude::*};

impl Session {
	pub(super) async fn finish_process_control_request(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::FinishClientRequestArg,
	) -> tg::Result<tg::process::control::FinishServerResponseOutput> {
		self.put_process_local(
			id,
			tg::process::put::Arg {
				data: arg.data,
				location: None,
			},
		)
		.await
		.map_err(|error| tg::error!(!error, %id, "failed to store the finished process"))?;
		self.server.enqueue_process_finalization(id).await?;
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
		// Spawn tasks to publish the stdio close messages.
		for stream in [
			tg::process::stdio::Stream::Stdin,
			tg::process::stdio::Stream::Stdout,
			tg::process::stdio::Stream::Stderr,
		] {
			self.server
				.spawn_publish_process_stdio_close_message_task(id, stream);
		}

		// Spawn a task to publish the status.
		self.server.spawn_publish_process_status_task(id);
	}
}
