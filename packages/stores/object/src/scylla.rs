use {
	crate::{CachePointer, DeleteArg, GrantArg, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	bytes::Bytes,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	std::{borrow::Cow, collections::HashMap},
	tangram_client::prelude::*,
};

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

	async fn object(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<crate::Object<'static>>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: Option<&'a [u8]>,
			cache_pointer: Option<&'a [u8]>,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?
		else {
			return Ok(None);
		};
		if row.bytes.is_none() && row.cache_pointer.is_none() {
			return Ok(None);
		}
		let bytes = row
			.bytes
			.map(|bytes| Cow::Owned(Bytes::copy_from_slice(bytes).to_vec()));
		let cache_pointer = row
			.cache_pointer
			.map(CachePointer::deserialize)
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the cache pointer"))?;
		Ok(Some(crate::Object {
			bytes,
			cache_pointer,
			stored_at: 0,
		}))
	}

	async fn objects(
		&self,
		ids: &[tg::object::Id],
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<HashMap<tg::object::Id, Bytes, tg::id::BuildHasher>> {
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (id_bytes,);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			bytes: Option<&'a [u8]>,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
		let map = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the rows"))?
			.filter_map(|result| {
				result
					.map_err(|error| tg::error!(!error, "failed to get the row"))
					.and_then(|row| {
						let Some(bytes) = row.bytes else {
							return Ok(None);
						};
						let id = tg::object::Id::from_slice(row.id)
							.map_err(|error| tg::error!(!error, "failed to parse the id"))?;
						let bytes = Bytes::copy_from_slice(bytes);
						Ok(Some((id, bytes)))
					})
					.transpose()
			})
			.collect::<tg::Result<_>>()?;
		Ok(map)
	}

	async fn grants(
		&self,
		id: &tg::object::Id,
		principal: &tg::Principal,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Vec<crate::Grant>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(Vec::new());
		}
		let params = (id.to_bytes().to_vec(), principal.to_string());
		#[derive(scylla::DeserializeRow)]
		struct Row {
			created_at: i64,
			subtree: bool,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		let grants = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, %id, "failed to iterate the rows"))?
			.map(|result| {
				let row =
					result.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?;
				Ok(crate::Grant {
					created_at: row.created_at,
					subtree: row.subtree,
				})
			})
			.collect::<tg::Result<_>>()?;
		Ok(grants)
	}

	async fn grants_batch(
		&self,
		ids: &[tg::object::Id],
		principal: &tg::Principal,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<HashMap<tg::object::Id, Vec<crate::Grant>, tg::id::BuildHasher>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(HashMap::default());
		}
		if ids.is_empty() {
			return Ok(HashMap::default());
		}
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (id_bytes, principal.to_string());
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			object: &'a [u8],
			created_at: i64,
			subtree: bool,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
		let mut grants = HashMap::default();
		for row in result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the rows"))?
		{
			let row = row.map_err(|error| tg::error!(!error, "failed to get the row"))?;
			let id = tg::object::Id::from_slice(row.object)
				.map_err(|error| tg::error!(!error, "failed to parse the id"))?;
			grants
				.entry(id)
				.or_insert_with(Vec::new)
				.push(crate::Grant {
					created_at: row.created_at,
					subtree: row.subtree,
				});
		}
		Ok(grants)
	}

	async fn try_get_with_statements(
		&self,
		arg: &TryGetArg,
		object_statement: &scylla::statement::prepared::PreparedStatement,
		grant_statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<TryGetOutput> {
		let object_future = self.object(&arg.id, object_statement);
		let grant_future = self.grants(&arg.id, &arg.principal, grant_statement);
		let (object, grants) = futures::try_join!(object_future, grant_future)?;
		Ok(TryGetOutput { grants, object })
	}

	async fn put_grant(
		&self,
		id: &tg::object::Id,
		principal: tg::Principal,
		subtree: bool,
		created_at: i64,
	) -> tg::Result<()> {
		let ttl = self.grant_ttl.to_i32().unwrap();
		let params = (
			id.to_bytes().to_vec(),
			principal.to_string(),
			subtree,
			created_at,
			ttl,
		);
		self.session
			.execute_unpaged(&self.statements.put_object_grant, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		let output = self
			.try_get_with_statements(
				&arg,
				&self.statements.get_object,
				&self.statements.get_object_grant,
			)
			.await?;
		if output.object.is_some()
			&& (matches!(arg.principal, tg::Principal::Root) || !output.grants.is_empty())
		{
			return Ok(output);
		}

		let mut object_statement = self.statements.get_object.clone();
		object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let mut grant_statement = self.statements.get_object_grant.clone();
		grant_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		self.try_get_with_statements(&arg, &object_statement, &grant_statement)
			.await
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let object_future = self.objects(&arg.ids, &self.statements.get_object_batch);
		let grant_future = self.grants_batch(
			&arg.ids,
			&arg.principal,
			&self.statements.get_object_grant_batch,
		);
		let (mut objects, mut grants) = futures::try_join!(object_future, grant_future)?;

		let missing = arg
			.ids
			.iter()
			.filter(|id| {
				!objects.contains_key(*id)
					|| (!matches!(arg.principal, tg::Principal::Root)
						&& grants.get(*id).is_none_or(std::vec::Vec::is_empty))
			})
			.cloned()
			.collect::<Vec<_>>();
		if !missing.is_empty() {
			let mut object_statement = self.statements.get_object_batch.clone();
			object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let mut grant_statement = self.statements.get_object_grant_batch.clone();
			grant_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let object_future = self.objects(&missing, &object_statement);
			let grant_future = self.grants_batch(&missing, &arg.principal, &grant_statement);
			let (missing_objects, missing_grants) =
				futures::try_join!(object_future, grant_future)?;
			objects.extend(missing_objects);
			grants.extend(missing_grants);
		}

		// Create the output.
		let output = arg
			.ids
			.iter()
			.map(|id| {
				let grants = grants.get(id).cloned().unwrap_or_default();
				let object = objects.get(id).cloned().map(|bytes| crate::Object {
					bytes: Some(Cow::Owned(bytes.to_vec())),
					cache_pointer: None,
					stored_at: 0,
				});
				TryGetOutput { grants, object }
			})
			.collect();

		Ok(output)
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = &arg.id;
		let bytes = arg.bytes;
		let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
			let cache_pointer = cache_pointer.serialize().map_err(
				|error| tg::error!(!error, %id, "failed to serialize the cache pointer"),
			)?;
			Some(cache_pointer)
		} else {
			None
		};
		let id_bytes = id.to_bytes().to_vec();
		let stored_at = arg.stored_at;
		let params = (bytes, cache_pointer, id_bytes, stored_at);
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		if let Some(principal) = arg.principal {
			self.put_grant(id, principal, false, stored_at).await?;
		}
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.statements.put_object.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.put_object.clone(),
			));
		}
		let params = args
			.iter()
			.map(|arg| {
				let id = &arg.id;
				let bytes = arg.bytes.clone();
				let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
					let cache_pointer = cache_pointer.serialize().map_err(
						|error| tg::error!(!error, %id, "failed to serialize the cache pointer"),
					)?;
					Some(cache_pointer)
				} else {
					None
				};
				let id_bytes = id.to_bytes().to_vec();
				let stored_at = arg.stored_at;
				let params = (bytes, cache_pointer, id_bytes, stored_at);
				Ok(params)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the batch"))?;
		futures::future::try_join_all(args.iter().filter_map(|arg| {
			arg.principal
				.clone()
				.map(|principal| self.put_grant(&arg.id, principal, false, arg.stored_at))
		}))
		.await?;
		Ok(())
	}

	async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		self.put_grant(&arg.id, arg.principal, arg.subtree, arg.created_at)
			.await
	}

	async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		futures::future::try_join_all(args.into_iter().map(|arg| async move {
			self.put_grant(&arg.id, arg.principal, arg.subtree, arg.created_at)
				.await
		}))
		.await?;
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let max_stored_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes, max_stored_at);
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		futures::future::try_join_all(args.into_iter().map(|arg| self.delete(arg))).await?;
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		Ok(())
	}
}
