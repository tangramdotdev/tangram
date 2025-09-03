use super::message::{
	DeleteTag, PutCacheEntry, PutObject, PutProcess, PutTagMessage, TouchObject, TouchProcess,
};
use crate::Server;
use futures::FutureExt as _;
use indoc::indoc;
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn indexer_task_handle_messages_sqlite(
		&self,
		database: &db::sqlite::Database,
		put_cache_entry_messages: Vec<PutCacheEntry>,
		put_object_messages: Vec<PutObject>,
		touch_object_messages: Vec<TouchObject>,
		put_process_messages: Vec<PutProcess>,
		touch_process_messages: Vec<TouchProcess>,
		put_tag_messages: Vec<PutTagMessage>,
		delete_tag_messages: Vec<DeleteTag>,
	) -> tg::Result<()> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with(move |connection| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				// Handle the messages.
				Self::indexer_put_cache_entries_sqlite(put_cache_entry_messages, &transaction)?;
				Self::indexer_put_objects_sqlite(put_object_messages, &transaction)?;
				Self::indexer_touch_objects_sqlite(touch_object_messages, &transaction)?;
				Self::indexer_put_processes_sqlite(put_process_messages, &transaction)?;
				Self::indexer_touch_processes_sqlite(touch_process_messages, &transaction)?;
				Self::indexer_put_tags_sqlite(put_tag_messages, &transaction)?;
				Self::indexer_delete_tags_sqlite(delete_tag_messages, &transaction)?;
				Self::indexer_increment_transaction_id_sqlite(&transaction)?;

				// Commit the transaction.
				transaction
					.commit()
					.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

				Ok::<_, tg::Error>(())
			})
			.await?;

		Ok(())
	}

	fn indexer_put_cache_entries_sqlite(
		messages: Vec<PutCacheEntry>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				insert into cache_entries (id, touched_at)
				values (?1, ?2)
				on conflict (id) do update set touched_at = ?2;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.id.to_string(), message.touched_at];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_put_objects_sqlite(
		messages: Vec<PutObject>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		// Prepare a statement for the objects.
		let objects_statement = indoc!(
			"
				insert into objects (id, cache_entry, complete, count, depth, size, touched_at, transaction_id, weight)
				values (?1, ?2, ?3, ?4, ?5, ?6, ?7, (select id from transaction_id), ?8)
				on conflict (id) do update set
					complete = complete or ?3,
					count = coalesce(count, ?4),
					depth = coalesce(depth, ?5),
					touched_at = coalesce(touched_at, ?7),
					weight = coalesce(weight, ?8);
			"
		);
		let mut objects_statement = transaction
			.prepare_cached(objects_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare a statement for the object children
		let children_statement = indoc!(
			"
				insert into object_children (object, child)
				values (?1, ?2)
				on conflict (object, child) do nothing;
			"
		);
		let mut children_statement = transaction
			.prepare_cached(children_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Execute inserts for each object in the batch.
			let id = message.id;
			let cache_entry = message.cache_entry.as_ref().map(ToString::to_string);
			let children = message.children;
			let complete = message.complete;
			let metadata = message.metadata;
			let size = message.size;
			let touched_at = message.touched_at;

			// Insert the children.
			for child in children {
				let child = child.to_string();
				let params = sqlite::params![&id.to_string(), &child];
				children_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the object.
			let params = sqlite::params![
				&id.to_string(),
				cache_entry,
				complete,
				metadata.count,
				metadata.depth,
				size,
				touched_at,
				metadata.weight
			];
			objects_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_touch_objects_sqlite(
		messages: Vec<TouchObject>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update objects
				set touched_at = ?1
				where id = ?2;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.touched_at, message.id.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_put_processes_sqlite(
		messages: Vec<PutProcess>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let process_statement = indoc!(
			"
				insert into processes (
					id,
					children_complete,
					children_count,
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight,
					touched_at,
					transaction_id
				)
				values (
					?1,
					?2,
					?3,
					?4,
					?5,
					?6,
					?7,
					?8,
					?9,
					?10,
					?11,
					?12,
					(select id from transaction_id)
				)
				on conflict (id) do update set
					children_complete = children_complete or ?2,
					children_count = coalesce(children_count, ?3),
					commands_complete = commands_complete or ?4,
					commands_count = coalesce(commands_count, ?5),
					commands_depth = coalesce(commands_depth, ?6),
					commands_weight = coalesce(commands_weight, ?7),
					outputs_complete = outputs_complete or ?8,
					outputs_count = coalesce(outputs_count, ?9),
					outputs_depth = coalesce(outputs_depth, ?10),
					outputs_weight = coalesce(outputs_weight, ?11),
					touched_at = ?12;
			"
		);
		let mut process_statement = transaction
			.prepare_cached(process_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let object_statement = indoc!(
			"
				insert into process_objects (process, object, kind)
				values (?1, ?2, ?3)
				on conflict (process, object, kind) do nothing;
			"
		);
		let mut object_statement = transaction
			.prepare_cached(object_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let child_statement = indoc!(
			"
				insert into process_children (process, position, child)
				values (?1, ?2, ?3)
				on conflict (process, child) do nothing;
			"
		);
		let mut child_statement = transaction
			.prepare_cached(child_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Insert the process.
			let params = sqlite::params![
				message.id.to_string(),
				message.complete.children,
				message.metadata.count,
				message.complete.commands,
				message.metadata.commands.count,
				message.metadata.commands.depth,
				message.metadata.commands.weight,
				message.complete.outputs,
				message.metadata.outputs.count,
				message.metadata.outputs.depth,
				message.metadata.outputs.weight,
				message.touched_at
			];
			process_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Insert the children.
			for (position, child) in message.children.iter().enumerate() {
				let params = sqlite::params![message.id.to_string(), position, child.to_string(),];
				child_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the objects.
			for (object, kind) in message.objects {
				let params =
					sqlite::params![message.id.to_string(), object.to_string(), kind.to_string()];
				object_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}

	fn indexer_touch_processes_sqlite(
		messages: Vec<TouchProcess>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update processes
				set touched_at = ?1
				where id = ?2;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.touched_at, message.id.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_put_tags_sqlite(
		messages: Vec<PutTagMessage>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				insert or replace into tags (tag, item)
				values (?1, ?2);
			"
		);
		let mut insert_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut objects_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut processes_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut cache_entries_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.tag, message.item.to_string()];
			insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let params = sqlite::params![message.item.to_string()];
			objects_reference_count_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			processes_reference_count_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			cache_entries_reference_count_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_delete_tags_sqlite(
		messages: Vec<DeleteTag>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				delete from tags
				where tag = ?1
				returning item;
			"
		);
		let mut delete_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count - 1
				where id = ?1
			"
		);
		let mut update_object_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count - 1
				where id = ?1
			"
		);
		let mut update_process_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count - 1
				where id = ?1
			"
		);
		let mut update_cache_entry_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.tag];
			let id = delete_statement
				.query_one(params, |row| row.get::<_, String>(0))
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let params = sqlite::params![id];
			update_object_reference_count_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			update_process_reference_count_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			update_cache_entry_reference_count_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn indexer_increment_transaction_id_sqlite(
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update transaction_id set id = id + 1;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		statement
			.execute([])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	pub(super) async fn indexer_handle_queue_sqlite(
		&self,
		config: &crate::config::Indexer,
		database: &db::sqlite::Database,
	) -> tg::Result<usize> {
		let batch_size = config.queue_batch_size;

		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let n = connection
			.with(move |connection| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				let mut n = batch_size;
				n -= Self::indexer_handle_complete_object_sqlite(&transaction, n)?;
				n -= Self::indexer_handle_complete_process_sqlite(&transaction, n)?;
				n -= Self::indexer_handle_reference_count_cache_entry_sqlite(&transaction, n)?;
				n -= Self::indexer_handle_reference_count_object_sqlite(&transaction, n)?;
				n -= Self::indexer_handle_reference_count_process_sqlite(&transaction, n)?;
				let n = batch_size - n;

				// Commit the transaction.
				if n > 0 {
					Self::indexer_increment_transaction_id_sqlite(&transaction)?;
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				Ok::<_, tg::Error>(n)
			})
			.await?;

		Ok(n)
	}

	fn indexer_handle_complete_object_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				delete from object_queue
				where id in (
					select id
					from object_queue
					where kind = 1
					order by id
					limit ?1
				)
				returning object, transaction_id;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;
		let mut rows = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?;

		struct Item {
			object: tg::object::Id,
			transaction_id: u64,
		}
		let mut items = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let object = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "failed to get the object from the row"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the object"))?;
			let transaction_id = row.get(1).map_err(|source| {
				tg::error!(!source, "failed to get the transaction id from the row")
			})?;
			let item = Item {
				object,
				transaction_id,
			};
			items.push(item);
		}
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				select complete
				from objects
				where id = ?1;
			"
		);
		let mut complete_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the complete statement"))?;

		let statement = indoc!(
			"
				insert into object_queue (object, kind, transaction_id)
				select object, 1, ?2
				from object_children
				join objects on objects.id = object_children.object
				where object_children.child = ?1 and objects.complete = 0;
			"
		);
		let mut enqueue_incomplete_parents_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue incomplete parents statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 2, ?2
				from process_objects
				join processes on processes.id = process_objects.process
				where process_objects.object = ?1 and processes.commands_complete = 0;
			"
		);
		let mut enqueue_incomplete_commands_processes_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue incomplete processes statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 3, ?2
				from process_objects
				join processes on processes.id = process_objects.process
				where process_objects.object = ?1 and processes.outputs_complete = 0;
			"
		);
		let mut enqueue_incomplete_outputs_processes_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue incomplete processes statement"
				)
			})?;

		let statement = indoc!(
			"
				update objects
				set
					complete = updates.complete,
					count = coalesce(objects.count, updates.count),
					depth = coalesce(objects.depth, updates.depth),
					weight = coalesce(objects.weight, updates.weight)
				from (
					select
						objects.id,
						coalesce(min(child_objects.complete), 1) as complete,
						1 + coalesce(sum(child_objects.count), 0) as count,
						1 + coalesce(max(child_objects.depth), 0) as depth,
						objects.size + coalesce(sum(child_objects.weight), 0) as weight
					from objects
					left join object_children on object_children.object = objects.id
					left join objects as child_objects on child_objects.id = object_children.child
					where objects.id = ?1
					group by objects.id, objects.size
					having coalesce(min(child_objects.complete), 1) = 1
				) as updates
				where objects.id = updates.id
				returning objects.complete;
			"
		);
		let mut update_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update complete statement")
			})?;

		for item in &items {
			// Get the object's complete flag.
			let params = [item.object.to_string()];
			let mut rows = complete_statement.query(params).map_err(|source| {
				tg::error!(!source, "failed to execute the complete statement")
			})?;
			let row = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the complete statement"))?
				.ok_or_else(|| tg::error!("expected a row"))?;
			let mut complete = row
				.get::<_, bool>(0)
				.map_err(|source| tg::error!(!source, "failed to deserialize the complete flag"))?;

			if !complete {
				// Update the object's complete flag.
				let params = [item.object.to_string()];
				let mut rows = update_complete_statement.query(params).map_err(|source| {
					tg::error!(!source, "failed to execute the update complete statement")
				})?;
				let row = rows
					.next()
					.map_err(|source| {
						tg::error!(!source, "failed to execute the update complete statement")
					})?
					.ok_or_else(|| tg::error!("expected a row"))?;
				complete = row.get::<_, bool>(0).map_err(|source| {
					tg::error!(!source, "failed to deserialize the complete flag")
				})?;
			}

			// If the object is complete, then enqueue incomplete parents and processes.
			if complete {
				let params = sqlite::params![item.object.to_string(), item.transaction_id];
				enqueue_incomplete_parents_statement
					.execute(params)
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to execute the enqueue incomplete parents statement"
						)
					})?;

				let params = sqlite::params![item.object.to_string(), item.transaction_id];
				enqueue_incomplete_commands_processes_statement
					.execute(params)
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to execute the enqueue incomplete processes statement"
						)
					})?;
				let params = sqlite::params![item.object.to_string(), item.transaction_id];
				enqueue_incomplete_outputs_processes_statement
					.execute(params)
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to execute the enqueue incomplete processes statement"
						)
					})?;
			}
		}

		Ok(items.len())
	}

	fn indexer_handle_complete_process_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				delete from process_queue
				where id in (
					select id
					from process_queue
					where kind >= 1
					order by id
					limit ?1
				)
				returning process, kind, transaction_id;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;
		let mut rows = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?;

		enum Kind {
			Children = 1,
			Commands = 2,
			Outputs = 3,
		}
		struct Item {
			process: tg::process::Id,
			kind: Kind,
			transaction_id: u64,
		}
		let mut items = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let process = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "failed to get the process from the row"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the process"))?;
			let kind = row
				.get::<_, u64>(1)
				.map_err(|source| tg::error!(!source, "failed to get the kind from the row"))?;
			let kind = match kind {
				1 => Kind::Children,
				2 => Kind::Commands,
				3 => Kind::Outputs,
				_ => return Err(tg::error!("invalid kind")),
			};
			let transaction_id = row.get(2).map_err(|source| {
				tg::error!(!source, "failed to get the transaction id from the row")
			})?;
			let item = Item {
				process,
				kind,
				transaction_id,
			};
			items.push(item);
		}
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				select children_complete
				from processes
				where id = ?1;
			"
		);
		let mut children_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the children complete statement")
			})?;
		let statement = indoc!(
			"
				select commands_complete
				from processes
				where id = ?1;
			"
		);
		let mut commands_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the commands complete statement")
			})?;
		let statement = indoc!(
			"
				select outputs_complete
				from processes
				where id = ?1;
			"
		);
		let mut outputs_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the outputs complete statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 1, ?2
				from process_children
				join processes on processes.id = process_children.process
				where
					process_children.child = ?1
					and processes.children_complete = 0;
			"
		);
		let mut enqueue_incomplete_children_parents_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue incomplete parents statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 2, ?2
				from process_children
				join processes on processes.id = process_children.process
				where
					process_children.child = ?1
					and processes.commands_complete = 0;
			"
		);
		let mut enqueue_incomplete_commands_parents_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue incomplete parents statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 3, ?2
				from process_children
				join processes on processes.id = process_children.process
				where
					process_children.child = ?1
					and processes.outputs_complete = 0;
			"
		);
		let mut enqueue_incomplete_outputs_parents_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue incomplete parents statement"
				)
			})?;

		let statement = indoc!(
			"
			update processes
			set
				children_complete = updates.children_complete,
				children_count = updates.children_count
			from (
				select
					processes.id,
					coalesce(min(child_processes.children_complete), 1) as children_complete,
					1 + coalesce(sum(child_processes.children_count), 0) as children_count
				from processes
				left join process_children on process_children.process = processes.id
				left join processes as child_processes on child_processes.id = process_children.child
				where processes.id = ?1
				group by processes.id
				having coalesce(min(child_processes.children_complete), 1) = 1
			) as updates
			where processes.id = updates.id
			returning children_complete;
		"
		);
		let mut update_children_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update complete statement")
			})?;

		let statement = indoc!(
			"
			update processes
				set
					commands_complete = updates.commands_complete,
					commands_count = updates.commands_count,
					commands_depth = updates.commands_depth,
					commands_weight = updates.commands_weight
				from (
					select
						processes.id,
						min(coalesce(min(command_objects.complete), 1), coalesce(min(child_processes.commands_complete), 1)) as commands_complete,
						coalesce(sum(command_objects.count), 0) + coalesce(sum(child_processes.commands_count), 0) as commands_count,
						max(coalesce(max(command_objects.depth), 0), coalesce(max(child_processes.commands_depth), 0)) as commands_depth,
						coalesce(sum(command_objects.weight), 0) + coalesce(sum(child_processes.commands_weight), 0) as commands_weight
					from processes
					left join process_objects process_objects_commands on process_objects_commands.process = processes.id and process_objects_commands.kind = 'command'
					left join objects command_objects on command_objects.id = process_objects_commands.object
					left join process_children process_children_commands on process_children_commands.process = processes.id
					left join processes child_processes on child_processes.id = process_children_commands.child
					where processes.id = ?1
					group by processes.id
					having min(coalesce(min(command_objects.complete), 1), coalesce(min(child_processes.commands_complete), 1)) = 1
				) as updates
				where processes.id = updates.id
				returning commands_complete;
			");
		let mut update_commands_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update complete statement")
			})?;

		let statement = indoc!(
			"
			update processes
			set
				outputs_complete = updates.outputs_complete,
				outputs_count = updates.outputs_count,
				outputs_depth = updates.outputs_depth,
				outputs_weight = updates.outputs_weight
				from (
					select
						processes.id,
						min(coalesce(min(output_objects.complete), 1), coalesce(min(child_processes.outputs_complete), 1)) as outputs_complete,
						coalesce(sum(output_objects.count), 0) + coalesce(sum(child_processes.outputs_count), 0) as outputs_count,
						max(coalesce(max(output_objects.depth), 0), coalesce(max(child_processes.outputs_depth), 0)) as outputs_depth,
						coalesce(sum(output_objects.weight), 0) + coalesce(sum(child_processes.outputs_weight), 0) as outputs_weight
					from processes
					left join process_objects process_objects_outputs on process_objects_outputs.process = processes.id and process_objects_outputs.kind = 'output'
					left join objects output_objects on output_objects.id = process_objects_outputs.object
					left join process_children process_children_outputs on process_children_outputs.process = processes.id
					left join processes child_processes on child_processes.id = process_children_outputs.child
					where processes.id = ?1
					group by processes.id
					having min(coalesce(min(output_objects.complete), 1), coalesce(min(child_processes.outputs_complete), 1)) = 1
				) as updates
				where processes.id = updates.id
				returning outputs_complete;
			");
		let mut update_outputs_complete_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update complete statement")
			})?;

		for item in &items {
			match item.kind {
				Kind::Children => {
					let params = [item.process.to_string()];
					let mut rows = children_complete_statement
						.query(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the complete statement")
						})?;
					let row = rows
						.next()
						.map_err(|source| {
							tg::error!(!source, "failed to execute the complete statement")
						})?
						.ok_or_else(|| tg::error!("expected a row"))?;
					let mut children_complete = row.get::<_, bool>(0).map_err(|source| {
						tg::error!(!source, "failed to deserialize the children complete flag")
					})?;

					if !children_complete {
						let params = [item.process.to_string()];
						let mut rows =
							update_children_complete_statement
								.query(params)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to execute the update complete statement"
									)
								})?;
						let row = rows
							.next()
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the update complete statement"
								)
							})?
							.ok_or_else(|| tg::error!("expected a row"))?;
						children_complete = row.get::<_, bool>(0).map_err(|source| {
							tg::error!(!source, "failed to deserialize the children complete flag")
						})?;
					}

					if children_complete {
						let params = sqlite::params![item.process.to_string(), item.transaction_id];
						enqueue_incomplete_children_parents_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the enqueue incomplete parents statement"
								)
							})?;
					}
				},
				Kind::Commands => {
					let params = [item.process.to_string()];
					let mut rows = commands_complete_statement
						.query(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the complete statement")
						})?;
					let row = rows
						.next()
						.map_err(|source| {
							tg::error!(!source, "failed to execute the complete statement")
						})?
						.ok_or_else(|| tg::error!("expected a row"))?;
					let mut commands_complete = row.get::<_, bool>(0).map_err(|source| {
						tg::error!(!source, "failed to deserialize the commands complete flag")
					})?;

					if !commands_complete {
						let params = [item.process.to_string()];
						let mut rows =
							update_commands_complete_statement
								.query(params)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to execute the update complete statement"
									)
								})?;
						let row = rows
							.next()
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the update complete statement"
								)
							})?
							.ok_or_else(|| tg::error!("expected a row"))?;
						commands_complete = row.get::<_, bool>(0).map_err(|source| {
							tg::error!(!source, "failed to deserialize the children complete flag")
						})?;
					}

					if commands_complete {
						let params = sqlite::params![item.process.to_string(), item.transaction_id];
						enqueue_incomplete_commands_parents_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the enqueue incomplete parents statement"
								)
							})?;
					}
				},
				Kind::Outputs => {
					let params = [item.process.to_string()];
					let mut rows = outputs_complete_statement.query(params).map_err(|source| {
						tg::error!(!source, "failed to execute the complete statement")
					})?;
					let row = rows
						.next()
						.map_err(|source| {
							tg::error!(!source, "failed to execute the complete statement")
						})?
						.ok_or_else(|| tg::error!("expected a row"))?;
					let mut outputs_complete = row.get::<_, bool>(0).map_err(|source| {
						tg::error!(!source, "failed to deserialize the commands complete flag")
					})?;

					if !outputs_complete {
						let params = [item.process.to_string()];
						let mut rows =
							update_outputs_complete_statement
								.query(params)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to execute the update complete statement"
									)
								})?;
						let row = rows
							.next()
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the update complete statement"
								)
							})?
							.ok_or_else(|| tg::error!("expected a row"))?;
						outputs_complete = row.get::<_, bool>(0).map_err(|source| {
							tg::error!(!source, "failed to deserialize the children complete flag")
						})?;
					}

					if outputs_complete {
						let params = sqlite::params![item.process.to_string(), item.transaction_id];
						enqueue_incomplete_outputs_parents_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the enqueue incomplete parents statement"
								)
							})?;
					}
				},
			}
		}

		Ok(items.len())
	}

	fn indexer_handle_reference_count_cache_entry_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				delete from cache_entry_queue
				where id in (
					select id
					from cache_entry_queue
					order by id
					limit ?1
				)
				returning cache_entry;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;
		let mut rows = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?;

		let mut ids = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let id: String = row
				.get(0)
				.map_err(|source| tg::error!(!source, "failed to get the ID from the row"))?;
			ids.push(id);
		}
		if ids.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				update cache_entries
				set
					reference_count = (
						select count(*)
						from objects
						where cache_entry = ?1
					),
					reference_count_transaction_id = (
						select id from transaction_id
					)
				where id = ?1;
			"
		);
		let mut reference_count_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the refence count statement")
			})?;

		for id in &ids {
			reference_count_statement.execute([id]).map_err(|source| {
				tg::error!(!source, "failed to execute the reference count statement")
			})?;
		}

		Ok(ids.len())
	}

	fn indexer_handle_reference_count_object_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				delete from object_queue
				where id in (
					select id
					from object_queue
					where kind = 0
					order by id
					limit ?1
				)
				returning object;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;
		let mut rows = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?;

		struct Item {
			object: tg::object::Id,
		}
		let mut items = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let object = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "failed to get the object from the row"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the object"))?;
			let item = Item { object };
			items.push(item);
		}
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				update objects
				set
					reference_count = (
						(select count(*) from object_children where child = ?1) +
						(select count(*) from process_objects where object = ?1) +
						(select count(*) from tags where item = ?1)
					),
					reference_count_transaction_id = (
						select id from transaction_id
					)
				where id = ?1;
			"
		);
		let mut reference_count_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the reference count statement")
			})?;

		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count + 1
				where
					id in (
						select child
						from object_children
						where object = ?1
					)
					and reference_count is not null
					and reference_count_transaction_id < (
						select transaction_id
						from objects
						where id = ?1
					);
			"
		);
		let mut children_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the children statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count + 1
				where
					id = (
						select cache_entry
						from objects
						where id = ?1
					)
					and reference_count is not null
					and reference_count_transaction_id < (
						select transaction_id
						from objects
						where id = ?1
					);
			"
		);
		let mut cache_entries_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the cache entries statement")
			})?;

		for item in &items {
			// Update the object's reference count.
			let params = [item.object.to_string()];
			reference_count_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the reference count statement")
				})?;

			// Increment the children's reference counts.
			let params = [item.object.to_string()];
			children_statement.execute(params).map_err(|source| {
				tg::error!(!source, "failed to execute the children statement")
			})?;

			// Update the cache entries' reference counts.
			let params = [item.object.to_string()];
			cache_entries_statement.execute(params).map_err(|source| {
				tg::error!(!source, "failed to execute the cache entries statement")
			})?;
		}

		Ok(items.len())
	}

	fn indexer_handle_reference_count_process_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				delete from process_queue
				where id in (
					select id
					from process_queue
					where kind = 0
					order by id
					limit ?1
				)
				returning process;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;
		let mut rows = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?;

		struct Item {
			process: tg::process::Id,
		}
		let mut items = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let process = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "failed to get the process from the row"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the process"))?;
			let item = Item { process };
			items.push(item);
		}
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				update processes
				set
					reference_count = (
						(select count(*) from process_children where child = ?1) +
						(select count(*) from tags where item = ?1)
					),
					reference_count_transaction_id = (
						select id from transaction_id
					)
				where id = ?1;
			"
		);
		let mut reference_count_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the reference count statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count + 1
				where
					id in (
						select child
						from process_children
						where process = ?1
					)
					and reference_count is not null
					and reference_count_transaction_id < (
						select transaction_id
						from processes
						where id = ?1
					);
			"
		);
		let mut children_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the children statement"))?;

		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count + 1
				where
					id in (
						select object
						from process_objects
						where process = ?1
					)
					and reference_count is not null
					and reference_count_transaction_id < (
						select transaction_id
						from processes
						where id = ?1
					);
			"
		);
		let mut objects_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the objects statement"))?;

		for item in &items {
			reference_count_statement
				.execute([item.process.to_string()])
				.map_err(|source| {
					tg::error!(!source, "failed to execute the reference count statement")
				})?;
			children_statement
				.execute([item.process.to_string()])
				.map_err(|source| {
					tg::error!(!source, "failed to execute the children statement")
				})?;
			objects_statement
				.execute([item.process.to_string()])
				.map_err(|source| tg::error!(!source, "failed to execute the objects statement"))?;
		}

		Ok(items.len())
	}

	pub(super) async fn indexer_get_transaction_id_sqlite(
		&self,
		database: &db::sqlite::Database,
	) -> tg::Result<u64> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Read,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let statement = indoc!(
			"
				select id from transaction_id;
			"
		);
		let params = db::params![];
		let id = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(id)
	}

	pub(super) async fn indexer_get_queue_size_sqlite(
		&self,
		database: &db::sqlite::Database,
		transaction_id: u64,
	) -> tg::Result<u64> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Read,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let statement = indoc!(
			"
				select
					(select count(*) from cache_entry_queue where transaction_id <= ?1) +
					(select count(*) from object_queue where transaction_id <= ?1) +
					(select count(*) from process_queue where transaction_id <= ?1);
			"
		);
		let params = db::params![transaction_id];
		let count = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(count)
	}
}

pub async fn migrate(database: &db::sqlite::Database) -> tg::Result<()> {
	let migrations = vec![migration_0000(database).boxed()];

	let connection = database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	let version =
		connection
			.with(|connection| {
				connection
					.pragma_query_value(None, "user_version", |row| {
						Ok(row.get_unwrap::<_, usize>(0))
					})
					.map_err(|source| tg::error!(!source, "failed to get the version"))
			})
			.await?;
	drop(connection);

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The index has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		connection
			.with(move |connection| {
				connection
					.pragma_update(None, "user_version", version + 1)
					.map_err(|source| tg::error!(!source, "failed to get the version"))
			})
			.await?;
	}

	Ok(())
}

async fn migration_0000(database: &db::sqlite::Database) -> tg::Result<()> {
	let sql = include_str!("./schema.sql");
	let connection = database
		.write_connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	connection
		.with(move |connection| {
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
