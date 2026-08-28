use {
	super::Store,
	crate::object,
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream},
	std::borrow::Cow,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn try_get_object(
		&self,
		arg: object::get::Arg,
	) -> tg::Result<object::get::Output> {
		let object = self
			.try_get_object_inner(&arg.id, &self.statements.get_object)
			.await?;
		if object.is_some() {
			return Ok(object::get::Output { object });
		}

		let mut object_statement = self.statements.get_object.clone();
		object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let object = self
			.try_get_object_inner(&arg.id, &object_statement)
			.await?;
		Ok(object::get::Output { object })
	}

	pub(super) async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		let mut output = stream::iter(arg.ids.into_iter().enumerate())
			.map(|(index, id)| async move {
				let arg = object::get::Arg { id };
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
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<object::Object<'static>>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: Option<&'a [u8]>,
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
		let Some(bytes) = row.bytes else {
			return Ok(None);
		};
		let bytes = Cow::Owned(Bytes::copy_from_slice(bytes).to_vec());
		Ok(Some(object::Object {
			bytes: Some(bytes),
			checkout_pointer: None,
			length: None,
			stored_at: row.stored_at,
		}))
	}
}
