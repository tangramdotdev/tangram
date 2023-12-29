use super::Server;
use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use rusqlite::params;
use tangram_client as tg;
use tangram_error::{return_error, Error, Result, WrapErr};
use tg::object;

impl Server {
	pub async fn get_object_exists(&self, id: &object::Id) -> Result<bool> {
		// Check if the object exists in the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let statement = "
				select count(*) != 0
				from objects
				where id = ?1;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![id.to_string()];
			let mut rows = statement
				.query(params)
				.wrap_err("Failed to execute the query.")?;
			let row = rows
				.next()
				.wrap_err("Failed to retrieve the row.")?
				.wrap_err("Expected a row.")?;
			let exists = row.get::<_, bool>(0).wrap_err("Expected a bool.")?;
			if !exists {
				break 'a;
			}
			return Ok(true);
		}

		// Check if the object exists on the remote server.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			if remote.get_object_exists(id).await? {
				return Ok(true);
			}
		}

		Ok(false)
	}

	pub async fn try_get_object(&self, id: &object::Id) -> Result<Option<Bytes>> {
		// Attempt to get the object from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let statement = "
				select bytes
				from objects
				where id = ?1;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![id.to_string()];
			let mut rows = statement
				.query(params)
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to retrieve the row.")? else {
				break 'a;
			};
			let bytes = row.get::<_, Vec<u8>>(0).wrap_err("Expected bytes.")?.into();
			return Ok(Some(bytes));
		}

		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Get the object from the remote server.
			let Some(bytes) = remote.try_get_object(id).await? else {
				break 'a;
			};

			// Add the object to the database.
			{
				let db = self.inner.database.pool.get().await;
				let statement = "
					insert into objects (id, bytes)
					values (?1, ?2)
					on conflict (id) do update set bytes = ?2;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let params = params![id.to_string(), bytes.to_vec()];
				statement
					.execute(params)
					.wrap_err("Failed to execute the query.")?;
			}

			return Ok(Some(bytes));
		}

		Ok(None)
	}

	pub async fn try_put_object(&self, id: &object::Id, bytes: &Bytes) -> Result<Vec<object::Id>> {
		// Deserialize the data.
		let data = tg::object::Data::deserialize(id.kind(), bytes)
			.wrap_err("Failed to deserialize the data.")?;

		// Verify the ID.
		let verified_id: tg::object::Id = tg::Id::new_blake3(data.kind().into(), bytes)
			.try_into()
			.unwrap();
		if id != &verified_id {
			return_error!("The ID does not match the data.");
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
		if !missing.is_empty() {
			return Ok(missing);
		}

		// Add the object to the database.
		{
			let db = self.inner.database.pool.get().await;
			let statement = "
				insert into objects (id, bytes)
				values (?1, ?2)
				on conflict (id) do update set bytes = ?2;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![id.to_string(), bytes.to_vec()];
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(vec![])
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
		return_error!("Not yet implemented.");
	}
}
