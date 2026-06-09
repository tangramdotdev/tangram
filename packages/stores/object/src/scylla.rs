use {
	crate::{DeleteArg, GrantArg, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	futures::FutureExt as _,
	indoc::indoc,
	tangram_client::prelude::*,
};

mod delete;
mod flush;
mod get;
mod grant;
mod put;

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
	pub connections: Option<usize>,
	pub grant_ttl: u64,
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
	grant_ttl: u64,
	statements: Statements,
	session: scylla::client::session::Session,
}

struct Statements {
	delete_object: scylla::statement::prepared::PreparedStatement,
	delete_object_grants: scylla::statement::prepared::PreparedStatement,
	get_object_batch: scylla::statement::prepared::PreparedStatement,
	get_object_grant_batch: scylla::statement::prepared::PreparedStatement,
	get_object_grant: scylla::statement::prepared::PreparedStatement,
	get_object: scylla::statement::prepared::PreparedStatement,
	put_object_grant: scylla::statement::prepared::PreparedStatement,
	put_object: scylla::statement::prepared::PreparedStatement,
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
				delete from object_grants
				where object = ?;
			"
		);
		let mut delete_object_grants = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the delete grants statement"))?;
		delete_object_grants.set_consistency(scylla::statement::Consistency::LocalQuorum);

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
				select object, created_at, subtree
				from object_grants
				where object in ? and principal = ?;
			"
		);
		let mut get_object_grant_batch = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the get grant batch statement")
		})?;
		get_object_grant_batch.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select created_at, subtree
				from object_grants
				where object = ? and principal = ?;
			"
		);
		let mut get_object_grant = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the get grant statement"))?;
		get_object_grant.set_consistency(scylla::statement::Consistency::One);

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
				insert into object_grants (object, principal, subtree, created_at)
				values (?, ?, ?, ?)
				using ttl ?;
			"
		);
		let mut put_object_grant = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put grant statement"))?;
		put_object_grant.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let scylla = Self {
			grant_ttl: config.grant_ttl,
			statements: Statements {
				delete_object,
				delete_object_grants,
				get_object_batch,
				get_object_grant_batch,
				get_object_grant,
				get_object,
				put_object_grant,
				put_object,
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

	async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		self.grant(arg).await
	}

	async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		self.grant_batch(args).await
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg).await
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		self.delete_batch(args).await
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush().await
	}
}
