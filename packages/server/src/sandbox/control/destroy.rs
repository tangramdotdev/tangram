use {
	crate::{Session, database},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
};

struct StoreDestroyedSandboxArg {
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

impl Session {
	pub(super) async fn destroy_sandbox_control_request(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::DestroyClientRequestArg,
		created_at: i64,
		runner: Option<tg::runner::Id>,
	) -> tg::Result<tg::sandbox::control::DestroyServerResponseOutput> {
		if arg.data.id != *id {
			return Err(tg::error!(%id, "the sandbox id does not match"));
		}
		if !arg.data.status.is_destroyed() {
			return Err(tg::error!(%id, "expected a destroyed sandbox"));
		}

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		self.store_destroyed_sandbox(id, &arg.data, created_at, runner.as_ref(), now)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to store the destroyed sandbox"))?;

		let put_sandbox = tangram_index::sandbox::put::Arg {
			created_at,
			data: Some(arg.data),
			id: id.clone(),
			runner,
			touched_at: now,
		};
		self.server
			.index_tasks
			.spawn(|_| {
				let server = self.server.clone();
				async move {
					let result = server
						.index
						.batch(tangram_index::batch::Arg {
							put_sandboxes: vec![put_sandbox],
							..Default::default()
						})
						.await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), "failed to put the destroyed sandbox to the index");
					}
				}
			})
			.detach();

		self.server.spawn_publish_sandbox_status_task(id);
		self.server.spawn_publish_sandbox_finalize_message_task();

		Ok(tg::sandbox::control::DestroyServerResponseOutput {})
	}

	async fn store_destroyed_sandbox(
		&self,
		id: &tg::sandbox::Id,
		data: &tg::sandbox::get::Output,
		created_at: i64,
		runner: Option<&tg::runner::Id>,
		now: i64,
	) -> tg::Result<()> {
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
		let runner = runner.map(ToString::to_string);
		let status = data.status.to_string();
		self.server
			.process_store
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
					Self::store_destroyed_sandbox_with_transaction(
						transaction,
						StoreDestroyedSandboxArg {
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
			.await?;
		Ok(())
	}

	async fn store_destroyed_sandbox_with_transaction(
		transaction: &database::Transaction<'_>,
		arg: StoreDestroyedSandboxArg,
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
