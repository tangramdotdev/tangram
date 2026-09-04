use {crate::object, futures::FutureExt as _, indoc::indoc, tangram_client::prelude::*};

mod cache;
mod capacity;
mod delete;
mod flush;
mod get;
mod indexer;
mod log;
mod put;
mod queue;

const OBJECT_CONCURRENCY: usize = 64;

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
	pub capacity: Option<CapacityConfig>,
	pub connections: Option<usize>,
	pub keepalive: bool,
	pub keyspace: String,
	pub partition_offset: u64,
	pub password: Option<String>,
	pub speculative_execution: Option<SpeculativeExecution>,
	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CapacityConfig {
	pub available_query: String,
	pub total_query: String,
	pub ttl: std::time::Duration,
	pub url: String,
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
	capacity: Option<capacity::Client>,
	partition_offset: u64,
	statements: Statements,
	session: scylla::client::session::Session,
}

struct Statements {
	contains_object: scylla::statement::prepared::PreparedStatement,
	delete_object: scylla::statement::prepared::PreparedStatement,
	delete_object_cache_entry: scylla::statement::prepared::PreparedStatement,
	get_object: scylla::statement::prepared::PreparedStatement,
	get_object_cache_entries: scylla::statement::prepared::PreparedStatement,
	get_object_for_put: scylla::statement::prepared::PreparedStatement,
	indexer: indexer::Statements,
	log: log::Statements,
	put_object: scylla::statement::prepared::PreparedStatement,
	put_object_cache_entry: scylla::statement::prepared::PreparedStatement,
	queue: queue::Statements,
}

impl Store {
	pub async fn new(config: &Config) -> tg::Result<Self> {
		physical_partition(0, config.partition_offset)?;

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
				where id = ? and put = ?;
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
				select bytes, put
				from objects
				where id = ?
				limit 1;
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
				select bytes, put
				from objects
				where id = ? and put = ?;
			"
		);
		let mut get_object_for_put = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the get object for put statement")
		})?;
		get_object_for_put.set_consistency(scylla::statement::Consistency::One);
		get_object_for_put.set_is_idempotent(true);

		let statement = indoc!(
			"
				select put
				from objects
				where id = ? and put = ?;
			"
		);
		let mut contains_object = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the contains object statement")
		})?;
		contains_object.set_consistency(scylla::statement::Consistency::One);
		contains_object.set_is_idempotent(true);

		let statement = indoc!(
			"
				insert into objects (bytes, id, put)
				values (?, ?, ?);
			"
		);
		let mut put_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put statement"))?;
		put_object.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put_object.set_is_idempotent(true);

		let statement = indoc!(
			"
				delete from object_cache
				where partition = ? and cache = ?;
			"
		);
		let mut delete_object_cache_entry = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the delete object cache entry statement"
			)
		})?;
		delete_object_cache_entry.set_consistency(scylla::statement::Consistency::LocalQuorum);
		delete_object_cache_entry.set_is_idempotent(true);

		let statement = indoc!(
			"
				select cache, object, partition, put
				from object_cache
				where partition = ?
				limit ?;
			"
		);
		let mut get_object_cache_entries = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the get object cache entries statement"
			)
		})?;
		get_object_cache_entries.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_object_cache_entries.set_is_idempotent(true);

		let statement = indoc!(
			"
				insert into object_cache (cache, object, partition, put)
				values (?, ?, ?, ?);
			"
		);
		let mut put_object_cache_entry = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the put object cache entry statement"
			)
		})?;
		put_object_cache_entry.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put_object_cache_entry.set_is_idempotent(true);

		if let Some(handle) = execution_profile {
			for statement in [
				&mut contains_object,
				&mut delete_object,
				&mut delete_object_cache_entry,
				&mut get_object,
				&mut get_object_cache_entries,
				&mut get_object_for_put,
				&mut put_object,
				&mut put_object_cache_entry,
			] {
				statement.set_execution_profile_handle(Some(handle.clone()));
			}
		}
		let indexer = indexer::Statements::new(&session).await?;
		let log = log::Statements::new(&session).await?;
		let queue = queue::Statements::new(&session).await?;

		let capacity = config
			.capacity
			.as_ref()
			.map(capacity::Client::new)
			.transpose()?;
		let scylla = Self {
			capacity,
			partition_offset: config.partition_offset,
			statements: Statements {
				contains_object,
				delete_object,
				delete_object_cache_entry,
				get_object,
				get_object_cache_entries,
				get_object_for_put,
				indexer,
				log,
				put_object,
				put_object_cache_entry,
				queue,
			},
			session,
		};

		Ok(scylla)
	}
}

impl crate::Store for Store {
	async fn contains_object(&self, arg: object::contains::Arg) -> tg::Result<bool> {
		self.contains_object(arg).await
	}

	async fn delete_indexer(&self, arg: crate::indexer::delete::Arg) -> tg::Result<()> {
		self.delete_indexer(arg).await
	}

	async fn delete_object_cache_entry(
		&self,
		arg: crate::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg).await
	}

	async fn delete_object_archive_queue_entry(
		&self,
		arg: crate::object::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_archive_queue_entry(arg).await
	}

	async fn delete_log(&self, arg: crate::log::delete::Arg) -> tg::Result<()> {
		self.delete_log_inner(arg).await
	}

	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn delete_object_index_queue_fragment(
		&self,
		arg: crate::object::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_index_queue_fragment(arg).await
	}

	async fn get_object_cache_entries(
		&self,
		arg: crate::object::cache::get::Arg,
	) -> tg::Result<Vec<crate::object::cache::Entry>> {
		self.get_object_cache_entries(arg).await
	}

	async fn get_indexers(&self) -> tg::Result<Vec<crate::indexer::Indexer>> {
		self.get_indexers().await
	}

	async fn put_indexer(&self, arg: crate::indexer::put::Arg) -> tg::Result<()> {
		self.put_indexer(arg).await
	}

	async fn put_object_cache_entry(&self, arg: crate::object::cache::put::Arg) -> tg::Result<()> {
		self.put_object_cache_entry(arg).await
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: crate::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg).await
	}

	async fn put_object_archive_queue_entry(
		&self,
		arg: crate::object::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_object_archive_queue_entry(arg).await
	}

	async fn put_object_index_queue_fragment(
		&self,
		arg: crate::object::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_object_index_queue_fragment(arg).await
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

	async fn try_get_indexer(
		&self,
		arg: crate::indexer::get::Arg,
	) -> tg::Result<Option<crate::indexer::Indexer>> {
		self.try_get_indexer(arg).await
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_object_archive_queue_entry(
		&self,
		arg: crate::object::archive::queue::get::Arg,
	) -> tg::Result<Option<crate::object::archive::queue::Entry>> {
		self.try_get_object_archive_queue_entry(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		self.try_get_object_batch(arg).await
	}

	async fn try_get_object_index_queue_fragment(
		&self,
		arg: crate::object::index::queue::get::Arg,
	) -> tg::Result<Option<crate::object::index::queue::Fragment>> {
		self.try_get_object_index_queue_fragment(arg).await
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<crate::capacity::Capacity>> {
		self.try_get_capacity().await
	}

	async fn try_read_log(
		&self,
		arg: crate::log::read::Arg,
	) -> tg::Result<Vec<crate::log::read::Entry<'static>>> {
		self.try_read_log_inner(arg).await
	}

	async fn update_indexer(&self, arg: crate::indexer::update::Arg) -> tg::Result<()> {
		self.update_indexer(arg).await
	}
}

fn logical_partition(partition: i64, offset: u64) -> tg::Result<u64> {
	let partition = u64::try_from(partition)
		.map_err(|_| tg::error!(%partition, "the physical object cache partition was negative"))?;
	let partition = partition.checked_sub(offset).ok_or_else(
		|| tg::error!(%offset, %partition, "the physical object cache partition preceded the configured offset"),
	)?;

	Ok(partition)
}

fn physical_partition(partition: u64, offset: u64) -> tg::Result<i64> {
	let partition = partition
		.checked_add(offset)
		.and_then(|partition| i64::try_from(partition).ok())
		.ok_or_else(
			|| tg::error!(%offset, %partition, "the physical object cache partition exceeded an i64"),
		)?;

	Ok(partition)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn partition_offset_round_trips() {
		let partition = physical_partition(7, 42).unwrap();
		assert_eq!(partition, 49);
		assert_eq!(logical_partition(partition, 42).unwrap(), 7);
	}

	#[test]
	fn partition_offset_rejects_overflow() {
		assert!(physical_partition(u64::MAX, 1).is_err());
		assert!(logical_partition(41, 42).is_err());
	}
}
