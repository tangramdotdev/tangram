use {crate::object, futures::FutureExt as _, indoc::indoc, tangram_client::prelude::*};

mod delete;
mod flush;
mod get;
mod log;
mod outbox;
mod put;

const OBJECT_CONCURRENCY: usize = 64;

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
	pub connections: Option<usize>,
	pub keepalive: bool,
	pub keyspace: String,
	pub partition_offset: u64,
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
	partition_offset: u64,
	statements: Statements,
	session: scylla::client::session::Session,
}

struct Statements {
	delete_object: scylla::statement::prepared::PreparedStatement,
	delete_outbox_fragment: scylla::statement::prepared::PreparedStatement,
	dequeue_outbox_fragments: scylla::statement::prepared::PreparedStatement,
	enqueue_outbox_fragment: scylla::statement::prepared::PreparedStatement,
	get_object: scylla::statement::prepared::PreparedStatement,
	log: log::Statements,
	put_object: scylla::statement::prepared::PreparedStatement,
	try_get_outbox_batch: scylla::statement::prepared::PreparedStatement,
	try_get_outbox_batch_at_or_before: scylla::statement::prepared::PreparedStatement,
}

impl Store {
	pub async fn new(config: &Config) -> tg::Result<Self> {
		physical_outbox_partition(0, config.partition_offset)?;

		let mut builder =
			scylla::client::session_builder::SessionBuilder::new().known_node(&config.addr);
		if !config.keepalive {
			builder.config.keepalive_interval = None;
			builder.config.keepalive_timeout = None;
		}
		if let (Some(username), Some(password)) = (&config.username, &config.password) {
			builder = builder.user(username, password);
		}
		let execution_profile = if let Some(speculative_execution) = &config.speculative_execution {
			let policy: std::sync::Arc<
				dyn scylla::policies::speculative_execution::SpeculativeExecutionPolicy,
			> = match speculative_execution {
				SpeculativeExecution::Percentile {
					max_retry_count,
					percentile,
				} => {
					let entry = scylla::policies::speculative_execution::PercentileSpeculativeExecutionPolicy {
						max_retry_count: *max_retry_count,
						percentile: *percentile,
					};
					std::sync::Arc::new(entry)
				},
				SpeculativeExecution::Simple {
					max_retry_count,
					retry_interval,
				} => {
					let entry =
						scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy {
							max_retry_count: *max_retry_count,
							retry_interval: *retry_interval,
						};
					std::sync::Arc::new(entry)
				},
			};
			let handle = scylla::client::execution_profile::ExecutionProfile::builder()
				.speculative_execution_policy(Some(policy))
				.build()
				.into_handle();
			Some(handle)
		} else {
			None
		};
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
				using timestamp ?
				where id = ?;
			"
		);
		let mut delete_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the delete statement"))?;
		delete_object.set_consistency(scylla::statement::Consistency::LocalQuorum);
		delete_object.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes
				from objects
				where id = ?;
			"
		);
		let mut get_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the get statement"))?;
		get_object.set_consistency(scylla::statement::Consistency::One);
		get_object.set_is_idempotent(true);

		let statement = indoc!(
			"
				insert into objects (bytes, id, stored_at)
				values (?, ?, ?)
				using timestamp ?;
			"
		);
		let mut put_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put statement"))?;
		put_object.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put_object.set_is_idempotent(true);

		let statement = indoc!(
			r#"
				delete from outbox
				where partition = ? and "batch" = ? and fragment = ?;
			"#
		);
		let mut delete_outbox_fragment = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the delete outbox fragment statement"
			)
		})?;
		delete_outbox_fragment.set_consistency(scylla::statement::Consistency::LocalQuorum);
		delete_outbox_fragment.set_is_idempotent(true);

		let statement = indoc!(
			r#"
				select "batch", fragment, partition, payload
				from outbox
				where partition in ?
				limit ?;
			"#
		);
		let mut dequeue_outbox_fragments = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the dequeue outbox fragments statement"
			)
		})?;
		dequeue_outbox_fragments.set_consistency(scylla::statement::Consistency::LocalQuorum);
		dequeue_outbox_fragments.set_is_idempotent(true);

		let statement = indoc!(
			r#"
				insert into outbox ("batch", fragment, partition, payload)
				values (?, ?, ?, ?);
			"#
		);
		let mut enqueue_outbox_fragment = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the enqueue outbox fragment statement"
			)
		})?;
		enqueue_outbox_fragment.set_consistency(scylla::statement::Consistency::LocalQuorum);
		enqueue_outbox_fragment.set_is_idempotent(true);

		let statement = indoc!(
			r#"
				select max("batch")
				from outbox
				where partition in ?;
			"#
		);
		let mut try_get_outbox_batch = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the get outbox batch statement")
		})?;
		try_get_outbox_batch.set_consistency(scylla::statement::Consistency::LocalQuorum);
		try_get_outbox_batch.set_is_idempotent(true);

		let statement = indoc!(
			r#"
				select max("batch")
				from outbox
				where partition in ? and "batch" <= ?;
			"#
		);
		let mut try_get_outbox_batch_at_or_before =
			session.prepare(statement).await.map_err(|error| {
				tg::error!(
					!error,
					"failed to prepare the bounded get outbox batch statement"
				)
			})?;
		try_get_outbox_batch_at_or_before
			.set_consistency(scylla::statement::Consistency::LocalQuorum);
		try_get_outbox_batch_at_or_before.set_is_idempotent(true);
		if let Some(handle) = execution_profile {
			for statement in [
				&mut delete_object,
				&mut delete_outbox_fragment,
				&mut dequeue_outbox_fragments,
				&mut enqueue_outbox_fragment,
				&mut get_object,
				&mut put_object,
				&mut try_get_outbox_batch,
				&mut try_get_outbox_batch_at_or_before,
			] {
				statement.set_execution_profile_handle(Some(handle.clone()));
			}
		}
		let log = log::Statements::new(&session).await?;

		let scylla = Self {
			partition_offset: config.partition_offset,
			statements: Statements {
				delete_object,
				delete_outbox_fragment,
				dequeue_outbox_fragments,
				enqueue_outbox_fragment,
				get_object,
				log,
				put_object,
				try_get_outbox_batch,
				try_get_outbox_batch_at_or_before,
			},
			session,
		};

		Ok(scylla)
	}
}

impl crate::Store for Store {
	async fn delete_log(&self, arg: crate::log::delete::Arg) -> tg::Result<()> {
		self.delete_log_inner(arg).await
	}

	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn delete_outbox_fragments(
		&self,
		arg: crate::outbox::fragment::delete::Arg,
	) -> tg::Result<()> {
		self.delete_outbox_fragments(arg).await
	}

	async fn dequeue_outbox_fragments(
		&self,
		arg: crate::outbox::fragment::dequeue::Arg,
	) -> tg::Result<Vec<crate::outbox::fragment::Fragment>> {
		self.dequeue_outbox_fragments(arg).await
	}

	async fn enqueue_outbox_batch(
		&self,
		arg: crate::outbox::batch::enqueue::Arg,
	) -> tg::Result<()> {
		self.enqueue_outbox_batch(arg).await
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush().await
	}

	async fn put_log(&self, arg: crate::log::put::Arg) -> tg::Result<()> {
		self.put_log_inner(arg).await
	}

	async fn put_log_batch(&self, args: Vec<crate::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch_inner(args).await
	}

	async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await
	}

	async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args).await
	}

	async fn try_get_log_length(&self, arg: crate::log::length::Arg) -> tg::Result<Option<u64>> {
		self.try_get_log_length_inner(arg).await
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		self.try_get_object_batch(arg).await
	}

	async fn try_get_outbox_batch_at_or_before(
		&self,
		arg: crate::outbox::batch::get::Arg,
	) -> tg::Result<Option<crate::outbox::batch::Id>> {
		self.try_get_outbox_batch_at_or_before(arg).await
	}

	async fn try_read_log(
		&self,
		arg: crate::log::read::Arg,
	) -> tg::Result<Vec<crate::log::read::Entry<'static>>> {
		self.try_read_log_inner(arg).await
	}
}

fn object_timestamp(stored_at: i64) -> tg::Result<i64> {
	stored_at
		.checked_mul(1_000_000)
		.ok_or_else(|| tg::error!(%stored_at, "the object timestamp is out of range"))
}

fn logical_outbox_partition(partition: i64, offset: u64) -> tg::Result<u64> {
	let partition = u64::try_from(partition)
		.map_err(|_| tg::error!(%partition, "the physical outbox partition was negative"))?;
	let partition = partition.checked_sub(offset).ok_or_else(
		|| tg::error!(%offset, %partition, "the physical outbox partition preceded the configured offset"),
	)?;

	Ok(partition)
}

fn physical_outbox_partition(partition: u64, offset: u64) -> tg::Result<i64> {
	let partition = partition
		.checked_add(offset)
		.and_then(|partition| i64::try_from(partition).ok())
		.ok_or_else(
			|| tg::error!(%offset, %partition, "the physical outbox partition exceeded an i64"),
		)?;

	Ok(partition)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn partition_offset_round_trips() {
		let partition = physical_outbox_partition(7, 42).unwrap();
		assert_eq!(partition, 49);
		assert_eq!(logical_outbox_partition(partition, 42).unwrap(), 7);
	}

	#[test]
	fn partition_offset_rejects_overflow() {
		assert!(physical_outbox_partition(u64::MAX, 1).is_err());
		assert!(logical_outbox_partition(41, 42).is_err());
	}
}
