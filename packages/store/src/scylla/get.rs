use {
	super::Store,
	crate::object,
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream},
	std::borrow::Cow,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn contains_object(&self, arg: object::contains::Arg) -> tg::Result<bool> {
		let contains = self
			.contains_object_inner(&arg, &self.statements.contains_object)
			.await?;
		if contains {
			return Ok(true);
		}

		let mut statement = self.statements.contains_object.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		self.contains_object_inner(&arg, &statement).await
	}

	pub(super) async fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> tg::Result<object::get::Output> {
		let statement = if arg.put.is_some() {
			&self.statements.get_object_for_put
		} else {
			&self.statements.get_object
		};
		let object = self.try_get_object_inner(&arg, statement).await?;
		if object.is_some() {
			return Ok(object::get::Output { object });
		}

		let mut object_statement = statement.clone();
		object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let object = self.try_get_object_inner(&arg, &object_statement).await?;
		Ok(object::get::Output { object })
	}

	pub(super) async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		let mut output = stream::iter(arg.ids.into_iter().enumerate())
			.map(|(index, id)| async move {
				let arg = object::get::Arg { id, put: None };
				let output = self.try_get_object(arg).await?;

				Ok::<_, tg::Error>((index, output))
			})
			.buffer_unordered(super::OBJECT_CONCURRENCY)
			.try_collect::<Vec<_>>()
			.await?;
		output.sort_unstable_by_key(|(index, _)| *index);
		let output = output.into_iter().map(|(_, output)| output).collect();

		Ok(output)
	}

	async fn try_get_object_inner(
		&self,
		arg: &object::get::Arg,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<object::Object<'static>>> {
		let id = &arg.id;
		let id_bytes = id.to_bytes();
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: Option<&'a [u8]>,
			put: &'a [u8],
		}
		let result = if let Some(put) = arg.put {
			let params = (id_bytes.as_ref(), put.as_slice());
			self.session
				.execute_unpaged(statement, params)
				.boxed()
				.await
		} else {
			let params = (id_bytes.as_ref(),);
			self.session
				.execute_unpaged(statement, params)
				.boxed()
				.await
		}
		.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
		.into_rows_result()
		.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?
		else {
			return Ok(None);
		};
		let Some(bytes) = row.bytes else {
			return Ok(None);
		};
		let bytes = Cow::Owned(Bytes::copy_from_slice(bytes).to_vec());
		let put = row
			.put
			.try_into()
			.map_err(|_| tg::error!(%id, "invalid object put"))?;
		Ok(Some(object::Object {
			bytes: Some(bytes),
			checkout_pointer: None,
			length: None,
			put,
		}))
	}

	async fn contains_object_inner(
		&self,
		arg: &object::contains::Arg,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<bool> {
		let id = &arg.id;
		let id_bytes = id.to_bytes();
		let params = (id_bytes.as_ref(), arg.put.as_slice());
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		let contains = result
			.maybe_first_row::<(Vec<u8>,)>()
			.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?
			.is_some();

		Ok(contains)
	}
}
