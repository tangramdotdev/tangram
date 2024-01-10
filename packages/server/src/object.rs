use super::Server;
use crate::params;
use bytes::Bytes;
use futures::{stream, TryStreamExt};
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tokio_stream::StreamExt;

impl Server {
	pub async fn get_object_exists(&self, id: &tg::object::Id) -> Result<bool> {
		if self.get_object_exists_local(id).await? {
			return Ok(true);
		}
		if self.get_object_exists_remote(id).await? {
			return Ok(true);
		}
		Ok(false)
	}

	async fn get_object_exists_local(&self, id: &tg::object::Id) -> Result<bool> {
		let db = self.inner.database.get().await?;
		let statement = "
			select count(*) != 0
			from objects
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let row = rows
			.next()
			.wrap_err("Failed to retrieve the row.")?
			.wrap_err("Expected a row.")?;
		let exists = row
			.get::<_, bool>(0)
			.wrap_err("Failed to deserialize the column.")?;
		Ok(exists)
	}

	async fn get_object_exists_remote(&self, id: &tg::object::Id) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		let exists = remote.get_object_exists(id).await?;
		Ok(exists)
	}

	pub async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<tg::object::GetOutput>> {
		if let Some(bytes) = self.try_get_object_local(id).await? {
			Ok(Some(bytes))
		} else if let Some(bytes) = self.try_get_object_remote(id).await? {
			Ok(Some(bytes))
		} else {
			Ok(None)
		}
	}

	async fn try_get_object_local(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<tg::object::GetOutput>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select bytes
			from objects
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let Some(row) = rows.next().wrap_err("Failed to retrieve the row.")? else {
			return Ok(None);
		};
		let bytes = row
			.get::<_, Vec<u8>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.into();
		let output = tg::object::GetOutput { bytes };
		Ok(Some(output))
	}

	async fn try_get_object_remote(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<tg::object::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};

		// Get the object from the remote server.
		let Some(output) = remote.try_get_object(id).await? else {
			return Ok(None);
		};

		// Add the object to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into objects (id, bytes)
				values (?1, ?2)
				on conflict (id) do update set bytes = ?2;
			";
			let params = params![id.to_string(), output.bytes.to_vec()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(Some(output))
	}

	pub async fn try_put_object(
		&self,
		id: &tg::object::Id,
		bytes: &Bytes,
	) -> Result<tg::object::PutOutput> {
		// Deserialize the data.
		let data = tg::object::Data::deserialize(id.kind(), bytes)
			.wrap_err("Failed to deserialize the data.")?;

		// Verify the ID.
		let verified_id: tg::object::Id = tg::Id::new_blake3(data.kind().into(), bytes)
			.try_into()
			.unwrap();
		if id != &verified_id {
			return Err(error!("The ID does not match the data."));
		}

		// Check if there are any missing children.
		let missing = stream::iter(data.children())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_object_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Add the object to the database if there are no missing objects.
		if missing.is_empty() {
			let db = self.inner.database.get().await?;
			let statement = "
				insert into objects (id, bytes)
				values (?1, ?2)
				on conflict (id) do update set bytes = ?2;
			";
			let params = params![id.to_string(), bytes.to_vec()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		let output = tg::object::PutOutput { missing };

		Ok(output)
	}

	pub async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.wrap_err("The server does not have a remote.")?;
		tg::object::Handle::with_id(id.clone())
			.push(self, remote.as_ref())
			.await
			.wrap_err("Failed to push the package.")?;
		Ok(())
	}

	#[allow(clippy::unused_async)]
	pub async fn pull_object(&self, _id: &tg::object::Id) -> Result<()> {
		Err(error!("Not yet implemented."))
	}
}
