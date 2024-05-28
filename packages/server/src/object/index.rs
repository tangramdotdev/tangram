use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tg::Handle as _;

impl Server {
	pub(crate) async fn index_task(&self) -> tg::Result<()> {
		while let Ok(id) = self.object_index_queue.receiver.recv().await {
			self.index_object(&id).await.ok();
		}
		Ok(())
	}

	async fn index_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		// Get the object.
		let tg::object::get::Output { bytes, .. } = self.get_object(id).await?;

		// Deserialize the object.
		let data = tg::object::Data::deserialize(id.kind(), &bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the current values for children, complete, count, and weight.
		#[derive(serde::Deserialize)]
		struct Row {
			children: bool,
			complete: bool,
			count: Option<u64>,
			weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select children, complete, count, weight
				from objects
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let Row {
			children,
			complete,
			count,
			weight,
		} = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// If the children were not set, then add them.
		if !children {
			// Get a database connection.
			let mut connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Begin a transaction.
			let transaction = connection
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

			// Add the children.
			for child in data.children() {
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into object_children (object, child)
						values ({p}1, {p}2)
						on conflict (object, child) do nothing;
					"
				);
				let params = db::params![id, child];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Set the children flag.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					update objects
					set children = 1
					where id = {p}1;
				"
			);
			let params = db::params![id];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Commit the transaction.
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

			// Drop the connection.
			drop(connection);
		}

		// If the count or weight are not set, then attempt to compute them.
		if count.is_none() || weight.is_none() {
			// Get the children's metadata.
			let children_metadata: Vec<Option<tg::object::Metadata>> = data
				.children()
				.into_iter()
				.map(|id| async move { self.try_get_object_metadata(&id).await })
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

			// Attempt to compute the count and weight.
			let count = children_metadata
				.iter()
				.map(|option| option.as_ref().and_then(|metadata| metadata.count))
				.sum::<Option<u64>>()
				.map(|count| count + 1);
			let weight = children_metadata
				.iter()
				.map(|option| option.as_ref().and_then(|metadata| metadata.count))
				.sum::<Option<u64>>()
				.map(|weight| weight + bytes.len().to_u64().unwrap());

			// Set the count and weight if they are availble.
			if count.is_some() || weight.is_some() {
				// Get a database connection.
				let connection =
					self.database.connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

				// Set the count and weight.
				let p = connection.p();
				let statement = formatdoc!(
					"
						update objects
						set count = {p}1, weight = {p}2
						where id = {p}3;
					"
				);
				let params = db::params![count, weight, id];
				connection
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				// Drop the connection.
				drop(connection);
			}
		}

		// If the object is not complete, then attempt to set the complete flag.
		let completed = if complete {
			false
		} else {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Attempt to set the complete flag.
			let p = connection.p();
			let statement = formatdoc!(
				"
					update objects
					set complete = (
						select case
							when count(*) = count(objects.complete) then coalesce(min(complete), 1)
							else 0
						end
						from object_children
						left join objects on objects.id = object_children.child
						where object_children.object = {p}1
					)
					where id = {p}1
					returning complete;
				"
			);
			let params = db::params![id];
			let completed = connection
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Drop the connection.
			drop(connection);

			completed
		};

		// If the object became complete, then add its incomplete parents to the object index queue.
		if completed {
			// Get a database connection.
			let connection = self
				.database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Get the incomplete parents.
			let p = connection.p();
			let statement = formatdoc!(
				"
					select object
					from object_children
					left join objects on objects.id = object_children.object
					where object_children.child = {p}1 and (objects.complete = 0 or objects.complete is null);
				"
			);
			let params = db::params![id];
			let incomplete = connection
				.query_all_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Add the incomplete parents to the object index queue.
			for id in incomplete {
				self.object_index_queue.sender.send(id).await.unwrap();
			}

			// Drop the connection.
			drop(connection);
		}

		Ok(())
	}
}
