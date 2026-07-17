use {
	crate::{Server, database},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
};

struct StoreLostSandboxArg {
	cpu: Option<i64>,
	created_at: i64,
	creator: Option<String>,
	hostname: Option<String>,
	id: tg::sandbox::Id,
	isolation: Option<String>,
	memory: Option<i64>,
	mounts: String,
	network: Option<String>,
	now: i64,
	owner: Option<String>,
	runner: Option<String>,
	status: String,
	ttl: Option<i64>,
}

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
				put_runners: vec![tangram_index::runner::put::Arg {
					id: runner.clone(),
					scheduler: None,
				}],
				..Default::default()
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
			if data.status.is_started() {
				data.children.get_or_insert_default();
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
					.await
					.map_err(
						|source| tg::error!(!source, %process, "failed to store the lost process"),
					)?;
				session.spawn_process_finish_tasks(&process);
			}

			// Remove the tokens before updating the index.
			data.remove_tokens();

			self.index
				.batch(tangram_index::batch::Arg {
					put_processes: vec![tangram_index::process::put::Arg {
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
					}],
					..Default::default()
				})
				.await
				.map_err(
					|source| tg::error!(!source, %process, "failed to update the lost process in the index"),
				)?;
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
		self.store_lost_sandbox(id, &indexed, now).await?;
		self.index
			.batch(tangram_index::batch::Arg {
				put_sandboxes: vec![tangram_index::sandbox::put::Arg {
					created_at: indexed.created_at,
					data: indexed.data,
					id: id.clone(),
					runner: indexed.runner,
					touched_at: now,
				}],
				..Default::default()
			})
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the lost sandbox in the index"),
			)?;

		self.spawn_publish_sandbox_status_task(id);
		self.spawn_publish_sandbox_finalize_message_task();

		Ok(())
	}

	async fn store_lost_sandbox(
		&self,
		id: &tg::sandbox::Id,
		indexed: &tangram_index::sandbox::Sandbox,
		now: i64,
	) -> tg::Result<()> {
		let data = indexed
			.data
			.as_ref()
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?;
		let cpu = data
			.cpu
			.map(i64::try_from)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to convert the sandbox cpu"))?;
		let memory = data
			.memory
			.map(i64::try_from)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to convert the sandbox memory"))?;
		let isolation = data
			.isolation
			.as_ref()
			.map(serde_json::to_string)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to serialize the sandbox isolation"))?;
		let mounts = serde_json::to_string(&data.mounts)
			.map_err(|error| tg::error!(!error, "failed to serialize the sandbox mounts"))?;
		let network = data
			.network
			.as_ref()
			.map(serde_json::to_string)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to serialize the sandbox network"))?;
		let ttl = data.ttl.map(|ttl| i64::try_from(ttl.as_secs()).unwrap());
		let id = id.clone();
		let creator = data.creator.as_ref().map(ToString::to_string);
		let hostname = data.hostname.clone();
		let owner = data.owner.as_ref().map(ToString::to_string);
		let runner = indexed.runner.as_ref().map(ToString::to_string);
		let status = data.status.to_string();
		let created_at = indexed.created_at;
		self.process_store
			.run(|transaction| {
				let creator = creator.clone();
				let hostname = hostname.clone();
				let id = id.clone();
				let isolation = isolation.clone();
				let mounts = mounts.clone();
				let network = network.clone();
				let owner = owner.clone();
				let runner = runner.clone();
				let status = status.clone();
				async move {
					Self::store_lost_sandbox_with_transaction(
						transaction,
						StoreLostSandboxArg {
							cpu,
							created_at,
							creator,
							hostname,
							id,
							isolation,
							memory,
							mounts,
							network,
							now,
							owner,
							runner,
							status,
							ttl,
						},
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to store the lost sandbox"))?;
		Ok(())
	}

	async fn store_lost_sandbox_with_transaction(
		transaction: &database::Transaction<'_>,
		arg: StoreLostSandboxArg,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into sandboxes (
					cpu, created_at, creator, finished_at, hostname, id, isolation, memory,
					mounts, network, owner, runner, started_at, status, ttl
				)
				values (
					{p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7, {p}8,
					{p}9, {p}10, {p}11, {p}12, {p}13, {p}14, {p}15
				)
				on conflict (id) do update set
					cpu = excluded.cpu,
					creator = excluded.creator,
					finished_at = excluded.finished_at,
					hostname = excluded.hostname,
					isolation = excluded.isolation,
					memory = excluded.memory,
					mounts = excluded.mounts,
					network = excluded.network,
					owner = excluded.owner,
					runner = excluded.runner,
					status = excluded.status,
					ttl = excluded.ttl;
			"
		);
		let params = db::params![
			arg.cpu,
			arg.created_at,
			arg.creator,
			arg.now,
			arg.hostname,
			arg.id.to_string(),
			arg.isolation,
			arg.memory,
			arg.mounts,
			arg.network,
			arg.owner,
			arg.runner,
			arg.created_at,
			arg.status,
			arg.ttl,
		];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = formatdoc!(
			"
				insert into sandbox_finalize_queue (created_at, sandbox, status)
				values ({p}1, {p}2, {p}3)
				on conflict (sandbox) do nothing;
			"
		);
		let params = db::params![arg.now, arg.id.to_string(), "created"];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}
}
