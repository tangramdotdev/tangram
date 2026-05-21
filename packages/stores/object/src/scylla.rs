use {
	crate::{CachePointer, DeleteArg, DeleteMembershipArg, PutArg},
	bytes::Bytes,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	scylla::serialize::{
		SerializationError,
		row::{RowSerializationContext, SerializeRow},
		writers::RowWriter,
	},
	std::{
		borrow::Cow,
		collections::{HashMap, HashSet},
	},
	tangram_client::prelude::*,
};

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
	delete_membership: scylla::statement::prepared::PreparedStatement,
	delete_object: scylla::statement::prepared::PreparedStatement,
	get_membership_any_namespace: scylla::statement::prepared::PreparedStatement,
	get_membership_exact: scylla::statement::prepared::PreparedStatement,
	get_membership_prefix: scylla::statement::prepared::PreparedStatement,
	get_object: scylla::statement::prepared::PreparedStatement,
	get_object_batch: scylla::statement::prepared::PreparedStatement,
	put_membership: scylla::statement::prepared::PreparedStatement,
	put_object: scylla::statement::prepared::PreparedStatement,
	put_object_bytes: scylla::statement::prepared::PreparedStatement,
	put_object_cache_pointer: scylla::statement::prepared::PreparedStatement,
	put_object_stored_at: scylla::statement::prepared::PreparedStatement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i8)]
enum MembershipKind {
	Public = 0,
	Namespace = 1,
}

enum PutBatchValue {
	Object {
		bytes: Bytes,
		cache_pointer: Bytes,
		id: Vec<u8>,
		stored_at: i64,
	},
	ObjectBytes {
		bytes: Bytes,
		id: Vec<u8>,
		stored_at: i64,
	},
	ObjectCachePointer {
		cache_pointer: Bytes,
		id: Vec<u8>,
		stored_at: i64,
	},
	ObjectStoredAt {
		id: Vec<u8>,
		stored_at: i64,
	},
	Membership {
		id: Vec<u8>,
		kind: i8,
		namespace: String,
		stored_at: i64,
	},
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MembershipQuery {
	AnyNamespace,
	Exact { kind: i8, namespace: String },
	Prefix { end: String, start: String },
}

struct MembershipStatements<'a> {
	any_namespace: &'a scylla::statement::prepared::PreparedStatement,
	exact: &'a scylla::statement::prepared::PreparedStatement,
	prefix: &'a scylla::statement::prepared::PreparedStatement,
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
				delete from memberships
				where id = ? and kind = ? and namespace = ? if stored_at <= ?;
			"
		);
		let mut delete_membership = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the delete membership statement")
		})?;
		delete_membership.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				delete from objects
				where id = ? if stored_at <= ?;
			"
		);
		let mut delete_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the delete statement"))?;
		delete_object.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select id
				from memberships
				where id in ? and kind = ?
				per partition limit 1;
			"
		);
		let mut get_membership_any_namespace =
			session.prepare(statement).await.map_err(|error| {
				tg::error!(
					!error,
					"failed to prepare the get any namespace membership statement"
				)
			})?;
		get_membership_any_namespace.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select id
				from memberships
				where id in ? and kind = ? and namespace = ?;
			"
		);
		let mut get_membership_exact = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the get exact membership statement"
			)
		})?;
		get_membership_exact.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select id
				from memberships
				where id in ? and kind = ? and namespace >= ? and namespace < ?
				per partition limit 1;
			"
		);
		let mut get_membership_prefix = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the get prefix membership statement"
			)
		})?;
		get_membership_prefix.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select id, bytes, cache_pointer, stored_at
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
				select bytes, cache_pointer, stored_at
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
				update objects
				set bytes = ?, cache_pointer = ?, stored_at = ?
				where id = ?;
			"
		);
		let mut put_object = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put statement"))?;
		put_object.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				update objects
				set bytes = ?, stored_at = ?
				where id = ?;
			"
		);
		let mut put_object_bytes = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put bytes statement"))?;
		put_object_bytes.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				update objects
				set cache_pointer = ?, stored_at = ?
				where id = ?;
			"
		);
		let mut put_object_cache_pointer = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the put cache pointer statement")
		})?;
		put_object_cache_pointer.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				update objects
				set stored_at = ?
				where id = ?;
			"
		);
		let mut put_object_stored_at = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put stored at statement"))?;
		put_object_stored_at.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				insert into memberships (id, kind, namespace, stored_at)
				values (?, ?, ?, ?);
			"
		);
		let mut put_membership = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the put membership statement")
		})?;
		put_membership.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let scylla = Self {
			statements: Statements {
				delete_membership,
				delete_object,
				get_membership_any_namespace,
				get_membership_exact,
				get_membership_prefix,
				get_object,
				get_object_batch,
				put_membership,
				put_object,
				put_object_bytes,
				put_object_cache_pointer,
				put_object_stored_at,
			},
			session,
		};

		Ok(scylla)
	}

	async fn try_get_object_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<crate::Object<'static>>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: Option<&'a [u8]>,
			cache_pointer: Option<&'a [u8]>,
			stored_at: i64,
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
			stored_at: row.stored_at,
		}))
	}

	async fn get_object_batch_inner(
		&self,
		ids: &[tg::object::Id],
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<HashMap<tg::object::Id, crate::Object<'static>, tg::id::BuildHasher>> {
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (id_bytes,);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			bytes: Option<&'a [u8]>,
			cache_pointer: Option<&'a [u8]>,
			stored_at: i64,
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
						let id = tg::object::Id::from_slice(row.id)
							.map_err(|error| tg::error!(!error, "failed to parse the id"))?;
						let bytes = row
							.bytes
							.map(|bytes| Cow::Owned(Bytes::copy_from_slice(bytes).to_vec()));
						let cache_pointer = row
							.cache_pointer
							.map(CachePointer::deserialize)
							.transpose()
							.map_err(
								|error| tg::error!(!error, %id, "failed to deserialize the cache pointer"),
							)?;
						let object = crate::Object {
							bytes,
							cache_pointer,
							stored_at: row.stored_at,
						};
						Ok(Some((id, object)))
					})
					.transpose()
			})
			.collect::<tg::Result<_>>()?;
		Ok(map)
	}

	async fn get_membership_ids_inner(
		&self,
		ids: &[tg::object::Id],
		queries: &[MembershipQuery],
		statements: MembershipStatements<'_>,
	) -> tg::Result<HashSet<tg::object::Id, tg::id::BuildHasher>> {
		if ids.is_empty() || queries.is_empty() {
			return Ok(HashSet::with_hasher(tg::id::BuildHasher));
		}
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let futures = queries
			.iter()
			.map(|query| self.get_membership_ids_for_query(&id_bytes, query, &statements));
		let sets = futures::future::try_join_all(futures).await?;
		let mut output = HashSet::with_hasher(tg::id::BuildHasher);
		for set in sets {
			output.extend(set);
		}
		Ok(output)
	}

	fn get_membership_ids_for_query<'a>(
		&'a self,
		id_bytes: &'a [Vec<u8>],
		query: &'a MembershipQuery,
		statements: &'a MembershipStatements<'a>,
	) -> futures::future::BoxFuture<'a, tg::Result<HashSet<tg::object::Id, tg::id::BuildHasher>>> {
		async move {
			#[derive(scylla::DeserializeRow)]
			struct Row<'a> {
				id: &'a [u8],
			}
			let id_bytes = id_bytes.to_vec();
			let result = match query {
				MembershipQuery::AnyNamespace => {
					self.session
						.execute_unpaged(
							statements.any_namespace,
							(id_bytes, MembershipKind::Namespace as i8),
						)
						.boxed()
						.await
				},
				MembershipQuery::Exact { kind, namespace } => {
					self.session
						.execute_unpaged(statements.exact, (id_bytes, *kind, namespace.clone()))
						.boxed()
						.await
				},
				MembershipQuery::Prefix { end, start } => {
					self.session
						.execute_unpaged(
							statements.prefix,
							(
								id_bytes,
								MembershipKind::Namespace as i8,
								start.clone(),
								end.clone(),
							),
						)
						.boxed()
						.await
				},
			}
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
			let mut readable = HashSet::with_hasher(tg::id::BuildHasher);
			for row in result
				.rows::<Row>()
				.map_err(|error| tg::error!(!error, "failed to iterate the rows"))?
			{
				let row = row.map_err(|error| tg::error!(!error, "failed to get the row"))?;
				let id = tg::object::Id::from_slice(row.id)
					.map_err(|error| tg::error!(!error, "failed to parse the id"))?;
				readable.insert(id);
			}
			Ok(readable)
		}
		.boxed()
	}

	fn membership_key(namespace: Option<&tg::Namespace>) -> (i8, String) {
		match namespace {
			Some(namespace) => (MembershipKind::Namespace as i8, namespace.to_string()),
			None => (MembershipKind::Public as i8, String::new()),
		}
	}

	fn membership_queries(namespaces: &[tg::Namespace], public: bool) -> Vec<MembershipQuery> {
		let mut queries = Vec::new();
		if public {
			Self::push_membership_query(
				&mut queries,
				MembershipQuery::Exact {
					kind: MembershipKind::Public as i8,
					namespace: String::new(),
				},
			);
		}
		if namespaces.iter().any(tg::Namespace::is_root) {
			Self::push_membership_query(&mut queries, MembershipQuery::AnyNamespace);
			return queries;
		}
		for namespace in namespaces {
			let namespace = namespace.to_string();
			Self::push_membership_query(
				&mut queries,
				MembershipQuery::Exact {
					kind: MembershipKind::Namespace as i8,
					namespace: namespace.clone(),
				},
			);
			let start = format!("{namespace}/");
			let end = Self::prefix_end(&start);
			Self::push_membership_query(&mut queries, MembershipQuery::Prefix { end, start });
		}
		queries
	}

	fn push_membership_query(queries: &mut Vec<MembershipQuery>, query: MembershipQuery) {
		if !queries.contains(&query) {
			queries.push(query);
		}
	}

	fn prefix_end(prefix: &str) -> String {
		let mut end = prefix.to_owned();
		end.push(char::MAX);
		end
	}

	fn put_values(arg: PutArg) -> tg::Result<(PutBatchValue, PutBatchValue)> {
		let PutArg {
			bytes,
			cache_pointer,
			id,
			namespace,
			stored_at,
		} = arg;
		let cache_pointer = cache_pointer
			.map(|cache_pointer| {
				cache_pointer.serialize().map_err(
					|error| tg::error!(!error, %id, "failed to serialize the cache pointer"),
				)
			})
			.transpose()?;
		let id_bytes = id.to_bytes().to_vec();
		let (kind, namespace) = Self::membership_key(namespace.as_ref());
		let object = match (bytes, cache_pointer) {
			(Some(bytes), Some(cache_pointer)) => PutBatchValue::Object {
				bytes,
				cache_pointer,
				id: id_bytes.clone(),
				stored_at,
			},
			(Some(bytes), None) => PutBatchValue::ObjectBytes {
				bytes,
				id: id_bytes.clone(),
				stored_at,
			},
			(None, Some(cache_pointer)) => PutBatchValue::ObjectCachePointer {
				cache_pointer,
				id: id_bytes.clone(),
				stored_at,
			},
			(None, None) => PutBatchValue::ObjectStoredAt {
				id: id_bytes.clone(),
				stored_at,
			},
		};
		let membership = PutBatchValue::Membership {
			id: id_bytes,
			kind,
			namespace,
			stored_at,
		};
		Ok((object, membership))
	}

	fn append_put_statement(
		batch: &mut scylla::statement::batch::Batch,
		statements: &Statements,
		value: &PutBatchValue,
	) {
		let statement = match value {
			PutBatchValue::Object { .. } => statements.put_object.clone(),
			PutBatchValue::ObjectBytes { .. } => statements.put_object_bytes.clone(),
			PutBatchValue::ObjectCachePointer { .. } => statements.put_object_cache_pointer.clone(),
			PutBatchValue::ObjectStoredAt { .. } => statements.put_object_stored_at.clone(),
			PutBatchValue::Membership { .. } => statements.put_membership.clone(),
		};
		batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
			statement,
		));
	}
}

impl SerializeRow for PutBatchValue {
	fn serialize(
		&self,
		ctx: &RowSerializationContext<'_>,
		writer: &mut RowWriter,
	) -> Result<(), SerializationError> {
		match self {
			Self::Object {
				bytes,
				cache_pointer,
				id,
				stored_at,
			} => (bytes, cache_pointer, stored_at, id).serialize(ctx, writer),
			Self::ObjectBytes {
				bytes,
				id,
				stored_at,
			} => (bytes, stored_at, id).serialize(ctx, writer),
			Self::ObjectCachePointer {
				cache_pointer,
				id,
				stored_at,
			} => (cache_pointer, stored_at, id).serialize(ctx, writer),
			Self::ObjectStoredAt { id, stored_at } => (stored_at, id).serialize(ctx, writer),
			Self::Membership {
				id,
				kind,
				namespace,
				stored_at,
			} => (id, kind, namespace, stored_at).serialize(ctx, writer),
		}
	}

	fn is_empty(&self) -> bool {
		false
	}
}

impl crate::Store for Store {
	async fn try_get(
		&self,
		id: &tg::object::Id,
		namespaces: Vec<tg::Namespace>,
		public: bool,
	) -> tg::Result<Option<crate::Object<'static>>> {
		let membership_queries = Self::membership_queries(&namespaces, public);
		let object = self.try_get_object_inner(id, &self.statements.get_object);
		let membership = self.get_membership_ids_inner(
			std::slice::from_ref(id),
			&membership_queries,
			MembershipStatements {
				any_namespace: &self.statements.get_membership_any_namespace,
				exact: &self.statements.get_membership_exact,
				prefix: &self.statements.get_membership_prefix,
			},
		);
		let (object, membership) = futures::try_join!(object, membership)?;
		if object.is_some() && membership.contains(id) {
			return Ok(object);
		}

		let mut object_statement = self.statements.get_object.clone();
		object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let mut membership_any_namespace_statement =
			self.statements.get_membership_any_namespace.clone();
		membership_any_namespace_statement
			.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let mut membership_exact_statement = self.statements.get_membership_exact.clone();
		membership_exact_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let mut membership_prefix_statement = self.statements.get_membership_prefix.clone();
		membership_prefix_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let object = self.try_get_object_inner(id, &object_statement);
		let membership = self.get_membership_ids_inner(
			std::slice::from_ref(id),
			&membership_queries,
			MembershipStatements {
				any_namespace: &membership_any_namespace_statement,
				exact: &membership_exact_statement,
				prefix: &membership_prefix_statement,
			},
		);
		let (object, membership) = futures::try_join!(object, membership)?;
		Ok(object.filter(|_| membership.contains(id)))
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
		namespaces: Vec<tg::Namespace>,
		public: bool,
	) -> tg::Result<Vec<Option<crate::Object<'static>>>> {
		if ids.is_empty() {
			return Ok(Vec::new());
		}
		let membership_queries = Self::membership_queries(&namespaces, public);
		let objects = self.get_object_batch_inner(ids, &self.statements.get_object_batch);
		let memberships = self.get_membership_ids_inner(
			ids,
			&membership_queries,
			MembershipStatements {
				any_namespace: &self.statements.get_membership_any_namespace,
				exact: &self.statements.get_membership_exact,
				prefix: &self.statements.get_membership_prefix,
			},
		);
		let (mut objects, mut memberships) = futures::try_join!(objects, memberships)?;
		let missing_objects = ids
			.iter()
			.filter(|id| !objects.contains_key(*id))
			.cloned()
			.collect::<Vec<_>>();
		let missing_memberships = ids
			.iter()
			.filter(|id| !memberships.contains(*id))
			.cloned()
			.collect::<Vec<_>>();
		if !missing_objects.is_empty() || !missing_memberships.is_empty() {
			let mut object_statement = self.statements.get_object_batch.clone();
			object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let mut membership_any_namespace_statement =
				self.statements.get_membership_any_namespace.clone();
			membership_any_namespace_statement
				.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let mut membership_exact_statement = self.statements.get_membership_exact.clone();
			membership_exact_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let mut membership_prefix_statement = self.statements.get_membership_prefix.clone();
			membership_prefix_statement
				.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let objects_fallback = async {
				if missing_objects.is_empty() {
					Ok(HashMap::with_hasher(tg::id::BuildHasher))
				} else {
					self.get_object_batch_inner(&missing_objects, &object_statement)
						.await
				}
			};
			let memberships_fallback = async {
				if missing_memberships.is_empty() {
					Ok(HashSet::with_hasher(tg::id::BuildHasher))
				} else {
					self.get_membership_ids_inner(
						&missing_memberships,
						&membership_queries,
						MembershipStatements {
							any_namespace: &membership_any_namespace_statement,
							exact: &membership_exact_statement,
							prefix: &membership_prefix_statement,
						},
					)
					.await
				}
			};
			let (objects_fallback, memberships_fallback) =
				futures::try_join!(objects_fallback, memberships_fallback)?;
			objects.extend(objects_fallback);
			memberships.extend(memberships_fallback);
		}
		let output = ids
			.iter()
			.map(|id| {
				memberships
					.contains(id)
					.then(|| objects.get(id).cloned())
					.flatten()
			})
			.collect();
		Ok(output)
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (object, membership) = Self::put_values(arg)?;
		let params = (object, membership);
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(scylla::statement::Consistency::LocalQuorum);
		Self::append_put_statement(&mut batch, &self.statements, &params.0);
		Self::append_put_statement(&mut batch, &self.statements, &params.1);
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let mut params = Vec::with_capacity(args.len() * 2);
		for arg in args {
			let (object, membership) = Self::put_values(arg)?;
			Self::append_put_statement(&mut batch, &self.statements, &object);
			Self::append_put_statement(&mut batch, &self.statements, &membership);
			params.push(object);
			params.push(membership);
		}
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the batch"))?;
		Ok(())
	}

	async fn delete_membership(&self, arg: DeleteMembershipArg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let (kind, namespace) = Self::membership_key(arg.namespace.as_ref());
		let max_stored_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes, kind, namespace, max_stored_at);
		self.session
			.execute_unpaged(&self.statements.delete_membership, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	async fn delete_membership_batch(&self, args: Vec<DeleteMembershipArg>) -> tg::Result<()> {
		futures::future::try_join_all(args.into_iter().map(|arg| self.delete_membership(arg)))
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

#[cfg(test)]
mod tests {
	use super::*;

	fn namespace(value: &str) -> tg::Namespace {
		value.parse().unwrap()
	}

	fn object(content: &'static [u8]) -> (tg::object::Id, Bytes) {
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		(id, bytes)
	}

	#[test]
	fn test_put_values_preserve_omitted_object_columns() {
		let (id, bytes) = object(b"hello");
		let (object, membership) = Store::put_values(PutArg {
			bytes: Some(bytes.clone()),
			cache_pointer: None,
			id: id.clone(),
			namespace: None,
			stored_at: 100,
		})
		.unwrap();
		assert!(matches!(object, PutBatchValue::ObjectBytes { .. }));
		assert!(matches!(membership, PutBatchValue::Membership { .. }));

		let (object, membership) = Store::put_values(PutArg {
			bytes: None,
			cache_pointer: None,
			id,
			namespace: Some(namespace("foo")),
			stored_at: 200,
		})
		.unwrap();
		assert!(matches!(object, PutBatchValue::ObjectStoredAt { .. }));
		assert!(matches!(membership, PutBatchValue::Membership { .. }));
	}

	#[test]
	fn test_membership_queries_distinguish_public_and_root() {
		let queries = Store::membership_queries(&[], true);
		assert_eq!(
			queries,
			vec![MembershipQuery::Exact {
				kind: MembershipKind::Public as i8,
				namespace: String::new(),
			}]
		);

		let queries = Store::membership_queries(&[tg::Namespace::root()], false);
		assert_eq!(queries, vec![MembershipQuery::AnyNamespace]);

		let queries = Store::membership_queries(&[tg::Namespace::root()], true);
		assert_eq!(
			queries,
			vec![
				MembershipQuery::Exact {
					kind: MembershipKind::Public as i8,
					namespace: String::new(),
				},
				MembershipQuery::AnyNamespace,
			]
		);
	}

	#[test]
	fn test_membership_queries_use_exact_and_slash_prefix() {
		let queries = Store::membership_queries(&[namespace("foo")], false);
		assert_eq!(
			queries,
			vec![
				MembershipQuery::Exact {
					kind: MembershipKind::Namespace as i8,
					namespace: "foo".to_owned(),
				},
				MembershipQuery::Prefix {
					start: "foo/".to_owned(),
					end: Store::prefix_end("foo/"),
				},
			]
		);
		assert!("foo/bar" >= "foo/");
		assert!("foo/bar" < Store::prefix_end("foo/").as_str());
		assert!("foobar" >= Store::prefix_end("foo/").as_str());
	}
}
