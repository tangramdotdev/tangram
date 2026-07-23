use {
	crate::Server, futures::FutureExt as _, tangram_client::prelude::*, tangram_index::prelude::*,
};

impl Server {
	pub(crate) async fn cleanup_lost_runner(
		&self,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<()> {
		let indexed = self
			.index
			.try_get_runner(runner)
			.await
			.map_err(|error| tg::error!(!error, %runner, "failed to get the runner"))?;
		if indexed.is_some_and(|runner| runner.scheduler.as_ref() != Some(scheduler)) {
			return Ok(());
		}
		let sandboxes =
			self.index.get_runner_sandboxes(runner).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		for sandbox in sandboxes {
			self.cleanup_lost_sandbox(&sandbox).await?;
		}

		self.index
			.batch(tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutRunner(
					tangram_index::runner::put::Arg {
						id: runner.clone(),
						scheduler: None,
					},
				)],
			})
			.await
			.map_err(
				|error| tg::error!(!error, %runner, "failed to update the runner in the index"),
			)?;

		Ok(())
	}

	async fn cleanup_lost_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error = tg::error::Data {
			code: Some(tg::error::Code::HeartbeatExpiration),
			message: Some("heartbeat expired".to_owned()),
			..Default::default()
		};
		let processes =
			self.index.get_sandbox_processes(id).await.map_err(
				|source| tg::error!(!source, %id, "failed to get the sandbox processes"),
			)?;
		for (process, indexed) in processes {
			let mut data = indexed
				.data
				.ok_or_else(|| tg::error!(%process, "missing the process data"))?;
			let finished = data.status.is_started();
			if finished {
				data.children.get_or_insert_default();
				data.cacheable = false;
				data.error = Some(tg::Either::Left(error.clone()));
				data.exit = Some(1);
				data.finished_at = Some(now);
				data.status = tg::process::Status::Finished;
				let mut context = self.context.clone();
				context.principal = tg::Principal::Process(process.clone());
				let session = self.session(&context);
				session
					.put_process_local(
						&process,
						tg::process::put::Arg {
							data: data.clone(),
							location: None,
						},
					)
					.boxed()
					.await
					.map_err(
						|source| tg::error!(!source, %process, "failed to store the lost process"),
					)?;
			}

			// Remove the tokens before updating the index.
			let data = data.without_tokens();

			self.index
				.batch(tangram_index::batch::Arg {
					items: vec![tangram_index::batch::Item::PutProcess(
						tangram_index::process::put::Arg {
							children: None,
							command: data.command.clone().into(),
							data: Some(data.clone()),
							error: None,
							id: process.clone(),
							log: None,
							metadata: indexed.metadata,
							output: None,
							parent: None,
							sandbox: Some(data.sandbox.clone()),
							stored: indexed.stored,
							time_to_touch: self.config.process.time_to_touch,
							touched_at: now,
						},
					)],
				})
				.await
				.map_err(
					|source| tg::error!(!source, %process, "failed to update the lost process in the index"),
				)?;
			if finished {
				self.enqueue_process_finalization(&process).await?;
				let session = self.session(&self.context);
				session.spawn_process_finish_tasks(&process);
			}
		}

		let mut indexed = self
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;
		let data = indexed
			.data
			.as_mut()
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?;
		data.status = tg::sandbox::Status::Destroyed;
		self.index
			.batch(tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(
					tangram_index::sandbox::put::Arg {
						created_at: indexed.created_at,
						data: indexed.data,
						id: id.clone(),
						runner: indexed.runner,
						touched_at: now,
					},
				)],
			})
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the lost sandbox in the index"),
			)?;
		self.enqueue_sandbox_finalization(id).await?;

		self.spawn_publish_sandbox_status_task(id);

		Ok(())
	}
}
