use {
	super::Store,
	crate::{CachePointer, TryGetArg, TryGetBatchArg, TryGetOutput},
	bytes::Bytes,
	futures::FutureExt as _,
	std::{borrow::Cow, collections::HashMap},
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
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

	async fn try_get_with_statements(
		&self,
		arg: &TryGetArg,
		object_statement: &scylla::statement::prepared::PreparedStatement,
		grant_statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<TryGetOutput> {
		let object_future = self.try_get_object(&arg.id, object_statement);
		let grant_future = self.try_get_grant(&arg.id, &arg.principal, grant_statement);
		let (object, grants) = futures::try_join!(object_future, grant_future)?;
		Ok(TryGetOutput { grants, object })
	}

	async fn try_get_object(
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

	async fn try_get_grant(
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

	pub(super) async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let object_future = self.try_get_object_batch(&arg.ids, &self.statements.get_object_batch);
		let grant_future = self.try_get_grant_batch(
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
			let object_future = self.try_get_object_batch(&missing, &object_statement);
			let grant_future = self.try_get_grant_batch(&missing, &arg.principal, &grant_statement);
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

	async fn try_get_object_batch(
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

	async fn try_get_grant_batch(
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
}
