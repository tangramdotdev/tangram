use {
	crate::Session, std::hash::BuildHasher as _, tangram_cache::Cache as _,
	tangram_client::prelude::*,
};

impl Session {
	pub(super) async fn finish_process_control_request(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::control::FinishClientRequestArg,
		sync: Option<&tg::Referent<tg::sync::Id>>,
	) -> tg::Result<tg::process::control::FinishServerResponseOutput> {
		self.server.spawn_publish_process_status_task(id);
		crate::checkpoint!(self.server, "process.control.finish", id = %id).await;

		// Associate the output with the incoming sync before publishing the finished process.
		if let Some(sync) = sync {
			Self::inherit_process_authorization_tokens_for_sync(&mut arg.data, sync);
		}

		// Prepare the cache expiration.
		let log_cache_entry = if arg.data.log.is_some() {
			let finished_at = arg
				.data
				.finished_at
				.ok_or_else(|| tg::error!("missing the process finish timestamp"))?;
			let expires_at = finished_at
				+ i64::try_from(self.server.config.process.log_time_to_live.as_secs()).unwrap();
			let partition = tg::id::BuildHasher.hash_one(id)
				% self.server.config.process.log_cache_partition_total;
			let entry = tangram_cache::log::cache::Entry {
				expires_at,
				partition,
				process: id.clone(),
			};
			Some(entry)
		} else {
			None
		};

		let options = crate::process::put::Options {
			defer_index: true,
			enqueue_log_compaction: true,
			location: None,
			store_data: true,
			sync: sync.cloned(),
		};
		self.put_finished_process_local(id, arg.data, options)
			.await?;
		// Register cache expiration before acknowledging Finish.
		if let Some(entry) = log_cache_entry {
			let arg = tangram_cache::log::cache::put::Arg { entry };
			self.server.cache.put_log_cache_entry(arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to register the log cache expiration"),
			)?;
		}
		self.server
			.spawn_publish_process_stdio_close_message_task(id, tg::process::stdio::Stream::Stdin);
		crate::checkpoint!(self.server, "process.control.finish.submitted", process = %id).await;

		Ok(tg::process::control::FinishServerResponseOutput {})
	}

	pub(crate) fn inherit_process_authorization_tokens_for_sync(
		data: &mut tg::process::Data,
		sync: &tg::Referent<tg::sync::Id>,
	) {
		let location = tg::Location::Local(tg::location::Local::default());
		let authorization_tokens = sync.options.tokens.for_location(&location);
		if let Some(output) = &mut data.output {
			Self::update_process_value_tokens(output, &mut |tokens, _| {
				tokens.inherit(&authorization_tokens);
			});
		}
		if let Some(log) = &mut data.log {
			log.options.tokens.inherit(&authorization_tokens);
		}
		if let Some(tg::Either::Right(error)) = &mut data.error {
			error.options.tokens.inherit(&authorization_tokens);
		}
	}

	pub(crate) async fn store_process_error(
		&self,
		error: tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>,
	) -> tg::Either<tg::error::Data, tg::Referent<tg::error::Id>> {
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
		let result = error.store_with_instance(self).await;
		match result {
			Ok(_) => tg::Either::Right(error.to_referent()),
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
