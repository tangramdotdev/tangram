use {
	crate::{DeleteArg, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	futures::FutureExt as _,
	indoc::indoc,
	tangram_client::prelude::*,
};

mod delete;
mod flush;
mod get;
mod outbox;
mod put;

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
	pub connections: Option<usize>,
	pub keyspace: String,
	pub password: Option<String>,
	pub speculative_execution: Option<SpeculativeExecution>,
	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub enum SpeculativeExecution {
	Percentile {
		max_retry_count: usize,
		percentile: f64,
	},
	Simple {
		max_retry_count: usize,
		retry_interval: std::time::Duration,
	},
}

pub struct Store {
	statements: Statements,
	session: scylla::client::session::Session,
}

struct Statements {
	delete_object: scylla::statement::prepared::PreparedStatement,
	delete_outbox: scylla::statement::prepared::PreparedStatement,
	dequeue_outbox: scylla::statement::prepared::PreparedStatement,
	enqueue_outbox: scylla::statement::prepared::PreparedStatement,
	get_object: scylla::statement::prepared::PreparedStatement,
	get_object_batch: scylla::statement::prepared::PreparedStatement,
	put_object: scylla::statement::prepared::PreparedStatement,
	try_get_outbox_id: scylla::statement::prepared::PreparedStatement,
	try_get_outbox_id_at_or_before: scylla::statement::prepared::PreparedStatement,
}

impl Store {
	pub async fn new(config: &Config) -> tg::Result<Self> {
		let mut builder =
			scylla::client::session_builder::SessionBuilder::new().known_node(&config.addr);
		if let (Some(username), Some(password)) = (&config.username, &config.password) {
			builder = builder.user(username, password);
		}
		if let Some(speculative_execution) = &config.speculative_execution {
			let policy: std::sync::Arc<
				dyn scylla::policies::speculative_execution::SpeculativeExecutionPolicy,
			> = match speculative_execution {
				SpeculativeExecution::Percentile {
					max_retry_count,
					percentile,
				} => std::sync::Arc::new(
					scylla::policies::speculative_execution::PercentileSpeculativeExecutionPolicy {
						max_retry_count: *max_retry_count,
						percentile: *percentile,
					},
				),
				SpeculativeExecution::Simple {
					max_retry_count,
					retry_interval,
				} => std::sync::Arc::new(
					scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy {
						max_retry_count: *max_retry_count,
						retry_interval: *retry_interval,
					},
				),
			};
			let handle = scylla::client::execution_profile::ExecutionProfile::builder()
				.speculative_execution_policy(Some(policy))
				.build()
				.into_handle();
			builder = builder.default_execution_profile_handle(handle);
		}
		if let Some(connections) = config.connections.and_then(std::num::NonZeroUsize::new) {
			builder = builder.pool_size(scylla::client::PoolSize::PerHost(connections));
		}
		let session = builder.build().boxed().await.map_err(
			|error| tg::error!(!error, addr = %config.addr, "failed to build the session"),
		)?;
		session.use_keyspace(&config.keyspace, true).await.map_err(
			|error| tg::error!(!error, keyspace = %config.keyspace, "failed to use the keyspace"),
		)?;

		let statement = indoc!(
			"
				delete from objects
				where id = ? if stored_at < ?;
			"
		);
		let mut delete_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the delete statement"))?;
		delete_object.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select id, bytes
				from objects
				where id in ?;
			"
		);
		let mut get_object_batch = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the get batch statement"))?;
		get_object_batch.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select bytes, cache_pointer
				from objects
				where id = ?;
			"
		);
		let mut get_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the get statement"))?;
		get_object.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into objects (bytes, cache_pointer, id, stored_at)
				values (?, ?, ?, ?);
			"
		);
		let mut put_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put statement"))?;
		put_object.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				delete from outbox
				where partition = ? and id = ?;
			"
		);
		let mut delete_outbox = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the delete outbox statement"))?;
		delete_outbox.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select id, partition, payload
				from outbox
				where partition in ?
				limit ?;
			"
		);
		let mut dequeue_outbox = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the dequeue outbox statement")
		})?;
		dequeue_outbox.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into outbox (partition, id, payload)
				values (?, now(), ?);
			"
		);
		let mut enqueue_outbox = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the enqueue outbox statement")
		})?;
		enqueue_outbox.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select max(id)
				from outbox
				where partition in ?;
			"
		);
		let mut try_get_outbox_id = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the get outbox id statement"))?;
		try_get_outbox_id.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select max(id)
				from outbox
				where partition in ? and id <= ?;
			"
		);
		let mut try_get_outbox_id_at_or_before =
			session.prepare(statement).await.map_err(|error| {
				tg::error!(
					!error,
					"failed to prepare the bounded get outbox id statement"
				)
			})?;
		try_get_outbox_id_at_or_before.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let scylla = Self {
			statements: Statements {
				delete_object,
				delete_outbox,
				dequeue_outbox,
				enqueue_outbox,
				get_object,
				get_object_batch,
				put_object,
				try_get_outbox_id,
				try_get_outbox_id_at_or_before,
			},
			session,
		};

		Ok(scylla)
	}
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		self.try_get(arg).await
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		self.try_get_batch(arg).await
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		self.put_batch(args).await
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg).await
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		self.delete_batch(args).await
	}

	async fn delete_outbox(&self, arg: crate::outbox::DeleteArg) -> tg::Result<()> {
		self.delete_outbox(arg).await
	}

	async fn dequeue_outbox(
		&self,
		arg: crate::outbox::DequeueArg,
	) -> tg::Result<Vec<crate::outbox::Item>> {
		self.dequeue_outbox(arg).await
	}

	async fn enqueue_outbox(&self, arg: crate::outbox::EnqueueArg) -> tg::Result<()> {
		self.enqueue_outbox(arg).await
	}

	async fn try_get_outbox_id_at_or_before(
		&self,
		arg: crate::outbox::TryGetIdArg,
	) -> tg::Result<Option<crate::outbox::Id>> {
		self.try_get_outbox_id_at_or_before(arg).await
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush().await
	}
}
