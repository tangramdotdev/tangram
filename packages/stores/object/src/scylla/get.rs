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
		let object = self
			.try_get_object(&arg.id, &self.statements.get_object)
			.await?;
		if object.is_some() {
			return Ok(TryGetOutput { object });
		}

		let mut object_statement = self.statements.get_object.clone();
		object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let object = self.try_get_object(&arg.id, &object_statement).await?;
		Ok(TryGetOutput { object })
	}

	pub(super) async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let mut objects = self
			.try_get_object_batch(&arg.ids, &self.statements.get_object_batch)
			.await?;

		let missing = arg
			.ids
			.iter()
			.filter(|id| !objects.contains_key(*id))
			.cloned()
			.collect::<Vec<_>>();
		if !missing.is_empty() {
			let mut object_statement = self.statements.get_object_batch.clone();
			object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let missing_objects = self
				.try_get_object_batch(&missing, &object_statement)
				.await?;
			objects.extend(missing_objects);
		}

		let output = arg
			.ids
			.iter()
			.map(|id| {
				let object = objects.get(id).cloned().map(|bytes| crate::Object {
					bytes: Some(Cow::Owned(bytes.to_vec())),
					cache_pointer: None,
					stored_at: 0,
				});
				TryGetOutput { object }
			})
			.collect();

		Ok(output)
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
}
