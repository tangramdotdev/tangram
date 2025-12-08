use {
	super::message::{
		DeleteTag, PutCacheEntry, PutObject, PutProcess, PutTagMessage, TouchObject, TouchProcess,
	},
	crate::Server,
	futures::FutureExt as _,
	indoc::indoc,
	itertools::Itertools as _,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	std::str::FromStr as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_either::Either,
};

impl Server {
	#[expect(clippy::too_many_arguments)]
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
		let insert_statement = indoc!(
			"
				insert into cache_entries (id, touched_at)
				values (?1, ?2)
				on conflict (id) do nothing;
			"
		);
		let mut insert_statement = transaction
			.prepare_cached(insert_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let update_statement = indoc!(
			"
				update cache_entries
				set touched_at = ?2
				where id = ?1;
			"
		);
		let mut update_statement = transaction
			.prepare_cached(update_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let queue_statement = indoc!(
			"
				insert into cache_entry_queue (cache_entry, transaction_id)
				values (?1, (select id from transaction_id));
			"
		);
		let mut queue_statement = transaction
			.prepare_cached(queue_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			let params = sqlite::params![message.id.to_bytes().to_vec(), message.touched_at];

			// Try to insert.
			let rows = insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let inserted = rows > 0;

			// If not inserted, update instead.
			if !inserted {
				let params = sqlite::params![message.id.to_bytes().to_vec(), message.touched_at];
				update_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Only insert into queue if this was a new entry.
			if inserted {
				let params = sqlite::params![message.id.to_bytes().to_vec()];
				queue_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}

	fn indexer_put_objects_sqlite(
		messages: Vec<PutObject>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		// Prepare insert statement for objects.
		let insert_statement = indoc!(
			"
				insert into objects (id, cache_entry, node_size, subtree_count, subtree_depth, subtree_size, subtree_stored, touched_at, transaction_id)
				values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, (select id from transaction_id))
				on conflict (id) do nothing;
			"
		);
		let mut insert_statement = transaction
			.prepare_cached(insert_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare update statement for objects.
		let update_statement = indoc!(
			"
				update objects
				set
					subtree_count = coalesce(subtree_count, ?4),
					subtree_depth = coalesce(subtree_depth, ?5),
					subtree_size = coalesce(subtree_size, ?6),
					subtree_stored = subtree_stored or ?7,
					touched_at = coalesce(touched_at, ?8)
				where id = ?1;
			"
		);
		let mut update_statement = transaction
			.prepare_cached(update_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare a statement for the object children.
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

		// Prepare statement for object queue.
		let queue_statement = indoc!(
			"
				insert into object_queue (object, kind, transaction_id)
				values (?1, ?2, (select id from transaction_id));
			"
		);
		let mut queue_statement = transaction
			.prepare_cached(queue_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Execute inserts for each object in the batch.
			let id = message.id;
			let cache_entry = message
				.cache_entry
				.as_ref()
				.map(|entry| entry.to_bytes().to_vec());
			let children = message.children;
			let stored = message.stored;
			let metadata = message.metadata;
			let node_size = metadata.node.size.unwrap();
			let touched_at = message.touched_at;

			// Insert the children.
			for child in children {
				let child = child.to_bytes().to_vec();
				let params = sqlite::params![&id.to_bytes().to_vec(), &child];
				children_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Try to insert the object.
			let params = sqlite::params![
				&id.to_bytes().to_vec(),
				cache_entry,
				node_size,
				metadata.subtree.count,
				metadata.subtree.depth,
				metadata.subtree.size,
				stored.subtree,
				touched_at,
			];
			let rows = insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let inserted = rows > 0;

			// If not inserted, update instead.
			if !inserted {
				let params = sqlite::params![
					&id.to_bytes().to_vec(),
					cache_entry,
					node_size,
					metadata.subtree.count,
					metadata.subtree.depth,
					metadata.subtree.size,
					stored.subtree,
					touched_at,
				];
				update_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Only insert into queue if this was a new object.
			if inserted {
				for kind in [0, 1] {
					let params = sqlite::params![&id.to_bytes().to_vec(), kind];
					queue_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
			}
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
			let params = sqlite::params![message.touched_at, message.id.to_bytes().to_vec()];
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
		// Prepare insert statement for processes.
		let insert_statement = indoc!(
			"
				insert into processes (
					id,
					node_command_count,
					node_command_depth,
					node_command_size,
					node_command_stored,
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored,
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
					?13,
					?14,
					?15,
					?16,
					?17,
					?18,
					?19,
					?20,
					(select id from transaction_id)
				)
				on conflict (id) do nothing;
			"
		);
		let mut insert_statement = transaction
			.prepare_cached(insert_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare update statement for processes.
		let update_statement = indoc!(
			"
				update processes
				set
					node_command_count = coalesce(node_command_count, ?2),
					node_command_depth = coalesce(node_command_depth, ?3),
					node_command_size = coalesce(node_command_size, ?4),
					node_command_stored = node_command_stored or ?5,
					node_output_count = coalesce(node_output_count, ?6),
					node_output_depth = coalesce(node_output_depth, ?7),
					node_output_size = coalesce(node_output_size, ?8),
					node_output_stored = node_output_stored or ?9,
					subtree_command_count = coalesce(subtree_command_count, ?10),
					subtree_command_depth = coalesce(subtree_command_depth, ?11),
					subtree_command_size = coalesce(subtree_command_size, ?12),
					subtree_command_stored = subtree_command_stored or ?13,
					subtree_output_count = coalesce(subtree_output_count, ?14),
					subtree_output_depth = coalesce(subtree_output_depth, ?15),
					subtree_output_size = coalesce(subtree_output_size, ?16),
					subtree_output_stored = subtree_output_stored or ?17,
					subtree_count = coalesce(subtree_count, ?18),
					subtree_stored = subtree_stored or ?19,
					touched_at = ?20
				where id = ?1;
			"
		);
		let mut update_statement = transaction
			.prepare_cached(update_statement)
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

		// Prepare statement for process queue.
		let queue_statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				values (?1, ?2, (select id from transaction_id));
			"
		);
		let mut queue_statement = transaction
			.prepare_cached(queue_statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Try to insert the process.
			let params = sqlite::params![
				message.id.to_bytes().to_vec(),
				message.metadata.node.command.count,
				message.metadata.node.command.depth,
				message.metadata.node.command.size,
				message.stored.node_command,
				message.metadata.node.output.count,
				message.metadata.node.output.depth,
				message.metadata.node.output.size,
				message.stored.node_output,
				message.metadata.subtree.command.count,
				message.metadata.subtree.command.depth,
				message.metadata.subtree.command.size,
				message.stored.subtree_command,
				message.metadata.subtree.output.count,
				message.metadata.subtree.output.depth,
				message.metadata.subtree.output.size,
				message.stored.subtree_output,
				message.metadata.subtree.process_count,
				message.stored.subtree,
				message.touched_at,
			];
			let rows = insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let inserted = rows > 0;

			// If not inserted, update instead.
			if !inserted {
				let params = sqlite::params![
					message.id.to_bytes().to_vec(),
					message.metadata.node.command.count,
					message.metadata.node.command.depth,
					message.metadata.node.command.size,
					message.stored.node_command,
					message.metadata.node.output.count,
					message.metadata.node.output.depth,
					message.metadata.node.output.size,
					message.stored.node_output,
					message.metadata.subtree.command.count,
					message.metadata.subtree.command.depth,
					message.metadata.subtree.command.size,
					message.stored.subtree_command,
					message.metadata.subtree.output.count,
					message.metadata.subtree.output.depth,
					message.metadata.subtree.output.size,
					message.stored.subtree_output,
					message.metadata.subtree.process_count,
					message.stored.subtree,
					message.touched_at,
				];
				update_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the children.
			for (position, child) in message.children.iter().enumerate() {
				let params = sqlite::params![
					message.id.to_bytes().to_vec(),
					position,
					child.to_bytes().to_vec(),
				];
				child_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the objects.
			for (object, kind) in message.objects {
				let params = sqlite::params![
					message.id.to_bytes().to_vec(),
					object.to_bytes().to_vec(),
					kind.to_i64().unwrap(),
				];
				object_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Only insert into queue if this was a new process.
			if inserted {
				for kind in [0, 1, 2, 3] {
					let params = sqlite::params![message.id.to_bytes().to_vec(), kind];
					queue_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
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
			let params = sqlite::params![message.touched_at, message.id.to_bytes().to_vec()];
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
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			item: Vec<u8>,
		}

		let statement = indoc!(
			"
				select item
				from tags
				where tag = ?1;
			"
		);
		let mut get_old_item_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

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
		let mut objects_increment_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut processes_increment_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut cache_entries_increment_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut objects_decrement_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut processes_decrement_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut cache_entries_decrement_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Query for an old item that is currently tagged with this tag.
			let params = sqlite::params![message.tag];
			let mut rows = get_old_item_statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let old_item = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
				.map(|row| {
					<Row as db::sqlite::row::Deserialize>::deserialize(row).map(|row| row.item)
				})
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;

			// Insert or replace the tag.
			let item = match &message.item {
				Either::Left(item) => item.to_bytes().to_vec(),
				Either::Right(item) => item.to_bytes().to_vec(),
			};
			let params = sqlite::params![message.tag, item];
			insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Update reference counts based on whether the tag is new, changed, or unchanged.
			match old_item {
				Some(old_item) if old_item != item => {
					// Decrement the old item's reference count.
					let params = sqlite::params![old_item];
					objects_decrement_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					processes_decrement_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					cache_entries_decrement_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					// Increment the new item's reference count.
					let params = sqlite::params![item];
					objects_increment_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					processes_increment_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					cache_entries_increment_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				},
				None => {
					let params = sqlite::params![item];
					objects_increment_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					processes_increment_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					cache_entries_increment_reference_count_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				},
				Some(_) => (),
			}
		}

		Ok(())
	}

	fn indexer_delete_tags_sqlite(
		messages: Vec<DeleteTag>,
		transaction: &sqlite::Transaction<'_>,
	) -> tg::Result<()> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			item: Vec<u8>,
		}

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
				where id = ?1;
			"
		);
		let mut update_object_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut update_process_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut update_cache_entry_reference_count_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			let params = sqlite::params![message.tag];
			let mut rows = delete_statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				continue;
			};
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			let id = row.item;
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
				n -= Self::indexer_handle_stored_object_sqlite(&transaction, n)?;
				n -= Self::indexer_handle_stored_process_sqlite(&transaction, n)?;
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

	fn indexer_handle_stored_object_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Item {
			#[tangram_database(try_from = "Vec<u8>")]
			object: tg::object::Id,
			transaction_id: u64,
		}

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

		let items = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Item as db::sqlite::row::Deserialize>::deserialize)
			.map(|result| {
				result.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.collect::<tg::Result<Vec<Item>>>()?;
		if items.is_empty() {
			return Ok(0);
		}

		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			subtree_stored: bool,
		}

		let statement = indoc!(
			"
				select subtree_stored
				from objects
				where id = ?1;
			"
		);
		let mut subtree_stored_statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the stored statement"))?;

		let statement = indoc!(
			"
				insert into object_queue (object, kind, transaction_id)
				select object, 1, ?2
				from object_children
				join objects on objects.id = object_children.object
				where object_children.child = ?1 and objects.subtree_stored = 0;
			"
		);
		let mut enqueue_parents_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue parents statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 2, ?2
				from process_objects
				join processes on processes.id = process_objects.process
				where process_objects.object = ?1 and processes.subtree_command_stored = 0;
			"
		);
		let mut enqueue_commands_processes_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue processes statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 3, ?2
				from process_objects
				join processes on processes.id = process_objects.process
				where process_objects.object = ?1 and processes.subtree_output_stored = 0;
			"
		);
		let mut enqueue_outputs_processes_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue processes statement")
			})?;

		let statement = indoc!(
			"
				update objects
				set
					subtree_stored = updates.subtree_stored,
					subtree_count = coalesce(objects.subtree_count, updates.subtree_count),
					subtree_depth = coalesce(objects.subtree_depth, updates.subtree_depth),
					subtree_size = coalesce(objects.subtree_size, updates.subtree_size)
				from (
					select
						objects.id,
						case
							when count(object_children.child) = 0
								then 1
							when min(coalesce(child_objects.subtree_stored, 0))
								then 1
							else 0
						end as subtree_stored,
					1 + coalesce(sum(coalesce(child_objects.subtree_count, 0)), 0) as subtree_count,
					1 + coalesce(max(coalesce(child_objects.subtree_depth, 0)), 0) as subtree_depth,
					objects.node_size + coalesce(sum(coalesce(child_objects.subtree_size, 0)), 0) as subtree_size
					from objects
					left join object_children on object_children.object = objects.id
					left join objects as child_objects on child_objects.id = object_children.child
					where objects.id = ?1
					and objects.subtree_stored = 0
					group by objects.id, objects.node_size
				) as updates
				where objects.id = updates.id
				and updates.subtree_stored = 1
				returning objects.subtree_stored;
			"
		);
		let mut update_subtree_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the update subtree stored statement"
				)
			})?;

		for item in &items {
			// Get the object's subtree_stored flag.
			let params = [item.object.to_bytes().to_vec()];
			let mut rows = subtree_stored_statement.query(params).map_err(|source| {
				tg::error!(!source, "failed to execute the subtree stored statement")
			})?;
			let row = rows
				.next()
				.map_err(|source| {
					tg::error!(!source, "failed to execute the subtree stored statement")
				})?
				.ok_or_else(|| tg::error!("expected a row"))?;
			let row =
				<Row as db::sqlite::row::Deserialize>::deserialize(row).map_err(|source| {
					tg::error!(!source, "failed to deserialize the subtree stored flag")
				})?;
			let mut subtree_stored = row.subtree_stored;

			if !subtree_stored {
				// Update the object's subtree_stored flag.
				let params = [item.object.to_bytes().to_vec()];
				let mut rows = update_subtree_stored_statement
					.query(params)
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to execute the update subtree stored statement"
						)
					})?;
				let row = rows.next().map_err(|source| {
					tg::error!(
						!source,
						"failed to execute the update subtree stored statement"
					)
				})?;
				subtree_stored = if let Some(row) = row {
					let row = <Row as db::sqlite::row::Deserialize>::deserialize(row).map_err(
						|source| {
							tg::error!(!source, "failed to deserialize the subtree stored flag")
						},
					)?;
					row.subtree_stored
				} else {
					false
				};
			}

			// If the object's subtree is stored, then enqueue parents and processes.
			if subtree_stored {
				let params = sqlite::params![item.object.to_bytes().to_vec(), item.transaction_id];
				enqueue_parents_statement
					.execute(params)
					.map_err(|source| {
						tg::error!(!source, "failed to execute the enqueue parents statement")
					})?;

				let params = sqlite::params![item.object.to_bytes().to_vec(), item.transaction_id];
				enqueue_commands_processes_statement
					.execute(params)
					.map_err(|source| {
						tg::error!(!source, "failed to execute the enqueue processes statement")
					})?;
				let params = sqlite::params![item.object.to_bytes().to_vec(), item.transaction_id];
				enqueue_outputs_processes_statement
					.execute(params)
					.map_err(|source| {
						tg::error!(!source, "failed to execute the enqueue processes statement")
					})?;
			}
		}

		Ok(items.len())
	}

	fn indexer_handle_stored_process_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Item {
			#[tangram_database(try_from = "Vec<u8>")]
			process: tg::process::Id,
			#[tangram_database(try_from = "u64")]
			kind: Kind,
			transaction_id: u64,
		}

		#[derive(derive_more::TryFrom)]
		#[try_from(repr)]
		#[repr(u64)]
		enum Kind {
			Children = 1,
			Commands = 2,
			Outputs = 3,
		}

		#[derive(db::sqlite::row::Deserialize)]
		struct SubtreeStoredRow {
			subtree_stored: bool,
		}

		#[derive(db::sqlite::row::Deserialize)]
		struct SubtreeCommandStoredRow {
			subtree_command_stored: bool,
		}

		#[derive(db::sqlite::row::Deserialize)]
		struct SubtreeOutputStoredRow {
			subtree_output_stored: bool,
		}

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

		let items = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Item as db::sqlite::row::Deserialize>::deserialize)
			.map(|row| row.map_err(|source| tg::error!(!source, "failed to deserialize the row")))
			.collect::<tg::Result<Vec<Item>>>()?;
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				select subtree_stored
				from processes
				where id = ?1;
			"
		);
		let mut subtree_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the subtree stored statement")
			})?;

		let statement = indoc!(
			"
				select subtree_command_stored
				from processes
				where id = ?1;
			"
		);
		let mut subtree_command_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the subtree command stored statement"
				)
			})?;

		let statement = indoc!(
			"
				select subtree_output_stored
				from processes
				where id = ?1;
			"
		);
		let mut subtree_output_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the subtree output stored statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 1, ?2
				from process_children
				join processes on processes.id = process_children.process
				where
					process_children.child = ?1
					and processes.subtree_stored = 0;
			"
		);
		let mut enqueue_parents_process_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents process statement"
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
					and processes.subtree_command_stored = 0;
			"
		);
		let mut enqueue_parents_command_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents command
				 statement"
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
					and processes.subtree_output_stored = 0;
			"
		);
		let mut enqueue_parents_output_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents output statement"
				)
			})?;

		let statement = indoc!(
			"
				update processes
				set
					subtree_stored = updates.subtree_stored,
					subtree_count = updates.subtree_count
				from (
					select
						processes.id,
					case
						when count(process_children.child) = 0
							then 1
						when min(coalesce(child_processes.subtree_stored, 0)) = 1
							then 1
						else 0
					end as subtree_stored,
					1 + coalesce(sum(coalesce(child_processes.subtree_count, 0)), 0) as subtree_count
					from processes
					left join process_children on process_children.process = processes.id
					left join processes as child_processes on child_processes.id = process_children.child
					where processes.id = ?1
					and processes.subtree_stored = 0
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.subtree_stored = 1
				returning subtree_stored;
			"
		);
		let mut update_subtree_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
					set
						subtree_command_stored = updates.subtree_command_stored,
						subtree_command_count = updates.subtree_command_count,
						subtree_command_depth = updates.subtree_command_depth,
						subtree_command_size = updates.subtree_command_size
				from (
					select
						processes.id,
						case
							when
								min(coalesce(command_objects.subtree_stored, 0))
								and (count(process_children_commands.child) = 0
								or min(coalesce(child_processes.subtree_command_stored, 0)))
								then 1
							else 0
						end as subtree_command_stored,
					coalesce(sum(coalesce(command_objects.subtree_count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_command_count, 0)), 0) as subtree_command_count,
					max(coalesce(command_objects.subtree_depth, 0),
					coalesce(child_processes.subtree_command_depth, 0)) as subtree_command_depth,
					coalesce(sum(coalesce(command_objects.subtree_size, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_command_size, 0)), 0) as subtree_command_size
					from processes
					left join process_objects process_objects_commands on process_objects_commands.process = processes.id and process_objects_commands.kind = 0
					left join objects command_objects on command_objects.id = process_objects_commands.object
					left join process_children process_children_commands on process_children_commands.process = processes.id
					left join processes child_processes on child_processes.id = process_children_commands.child
					where processes.id = ?1
					and processes.subtree_command_stored = 0
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.subtree_command_stored = 1
				returning subtree_command_stored;
			"
		);
		let mut update_subtree_command_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set
					node_command_stored = objects.subtree_stored,
					node_command_count = objects.subtree_count,
					node_command_depth = objects.subtree_depth,
					node_command_size = objects.subtree_size
				from process_objects
				left join objects on process_objects.object = objects.id
				where processes.id = process_objects.process
					and process_objects.kind = 0
					and process_objects.process = ?1
					and objects.subtree_stored = 1
					and processes.node_command_stored = 0;
			"
		);
		let mut update_node_command_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set
					subtree_output_stored = updates.subtree_output_stored,
					subtree_output_count = updates.subtree_output_count,
					subtree_output_depth = updates.subtree_output_depth,
					subtree_output_size = updates.subtree_output_size
				from (
					select
						processes.id,
						case
							when
								min(coalesce(output_objects.subtree_stored, 0))
								and (count(process_children_outputs.child) = 0
								or min(coalesce(child_processes.subtree_output_stored, 0)))
								then 1
							else 0
						end as subtree_output_stored,
					coalesce(sum(coalesce(output_objects.subtree_count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_output_count, 0)), 0) as subtree_output_count,
					max(coalesce(output_objects.subtree_depth, 0),
					coalesce(child_processes.subtree_output_depth, 0)) as subtree_output_depth,
					coalesce(sum(coalesce(output_objects.subtree_size, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_output_size, 0)), 0) as subtree_output_size
					from processes
					left join process_objects process_objects_outputs on process_objects_outputs.process = processes.id and process_objects_outputs.kind = 3
					left join objects output_objects on output_objects.id = process_objects_outputs.object
					left join process_children process_children_outputs on process_children_outputs.process = processes.id
					left join processes child_processes on child_processes.id = process_children_outputs.child
					where processes.id = ?1
					and processes.subtree_output_stored = 0
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.subtree_output_stored = 1
				returning subtree_output_stored;
			"
		);
		let mut update_subtree_output_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set
					node_output_stored = updates.node_output_stored,
					node_output_count = updates.node_output_count,
					node_output_depth = updates.node_output_depth,
					node_output_size = updates.node_output_size
				from (
					select
						process_objects.process as id,
						case
							when count(process_objects.object) = 0 then 1
							when min(coalesce(objects.subtree_stored, 0)) = 1 then 1
							else 0
						end as node_output_stored,
						coalesce(sum(objects.subtree_count), 0) as node_output_count,
						coalesce(max(objects.subtree_depth), 0) as node_output_depth,
						coalesce(sum(objects.subtree_size), 0) as node_output_size
					from processes
					join process_objects on processes.id = process_objects.process
					left join objects on process_objects.object = objects.id
					where process_objects.kind = 3
					and process_objects.process = ?1
					and processes.node_output_stored = 0
					group by process_objects.process
				) updates
				where processes.id = updates.id
				and updates.node_output_stored = 1;
			"
		);
		let mut update_node_output_stored_statement =
			transaction.prepare_cached(statement).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		for item in &items {
			match item.kind {
				Kind::Children => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows = subtree_stored_statement.query(params).map_err(|source| {
						tg::error!(!source, "failed to execute the stored statement")
					})?;
					let row = rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the stored statement")
					})?;
					let mut subtree_stored = if let Some(row) = row {
						let row =
							<SubtreeStoredRow as db::sqlite::row::Deserialize>::deserialize(row)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to deserialize the subtree stored flag"
									)
								})?;
						row.subtree_stored
					} else {
						false
					};

					if !subtree_stored {
						let params = [item.process.to_bytes().to_vec()];
						let mut rows =
							update_subtree_stored_statement
								.query(params)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to execute the update stored statement"
									)
								})?;
						let row = rows.next().map_err(|source| {
							tg::error!(!source, "failed to execute the update stored statement")
						})?;
						subtree_stored = if let Some(row) = row {
							let row =
								<SubtreeStoredRow as db::sqlite::row::Deserialize>::deserialize(
									row,
								)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to deserialize the subtree stored flag"
									)
								})?;
							row.subtree_stored
						} else {
							false
						}
					}

					if subtree_stored {
						let params =
							sqlite::params![item.process.to_bytes().to_vec(), item.transaction_id];
						enqueue_parents_process_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the enqueue parents statement"
								)
							})?;
					}
				},
				Kind::Commands => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows =
						subtree_command_stored_statement
							.query(params)
							.map_err(|source| {
								tg::error!(!source, "failed to execute the stored statement")
							})?;
					let row = rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the stored statement")
					})?;
					let mut subtree_command_stored =
						if let Some(row) = row {
							let row =
							<SubtreeCommandStoredRow as db::sqlite::row::Deserialize>::deserialize(row)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to deserialize the commands stored flag"
									)
								})?;
							row.subtree_command_stored
						} else {
							false
						};

					if !subtree_command_stored {
						let params = [item.process.to_bytes().to_vec()];
						let mut rows = update_subtree_command_stored_statement
							.query(params)
							.map_err(|source| {
								tg::error!(!source, "failed to execute the update stored statement")
							})?;
						let row = rows.next().map_err(|source| {
							tg::error!(!source, "failed to execute the update stored statement")
						})?;
						subtree_command_stored = if let Some(row) = row {
							let row =
								<SubtreeCommandStoredRow as db::sqlite::row::Deserialize>::deserialize(
									row,
								)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to deserialize the subtree command stored flag"
									)
								})?;
							row.subtree_command_stored
						} else {
							false
						};

						// Update command stored.
						let params = [item.process.to_bytes().to_vec()];
						update_node_command_stored_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the update command stored statement"
								)
							})?;
					}

					if subtree_command_stored {
						let params =
							sqlite::params![item.process.to_bytes().to_vec(), item.transaction_id];
						enqueue_parents_command_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the enqueue parents statement"
								)
							})?;
					}
				},
				Kind::Outputs => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows =
						subtree_output_stored_statement
							.query(params)
							.map_err(|source| {
								tg::error!(!source, "failed to execute the stored statement")
							})?;
					let row = rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the stored statement")
					})?;
					let mut subtree_output_stored = if let Some(row) = row {
						let row =
							<SubtreeOutputStoredRow as db::sqlite::row::Deserialize>::deserialize(
								row,
							)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the outputs stored flag")
							})?;
						row.subtree_output_stored
					} else {
						false
					};

					if !subtree_output_stored {
						let params = [item.process.to_bytes().to_vec()];
						let mut rows = update_subtree_output_stored_statement
							.query(params)
							.map_err(|source| {
								tg::error!(!source, "failed to execute the update stored statement")
							})?;
						let row = rows.next().map_err(|source| {
							tg::error!(!source, "failed to execute the update stored statement")
						})?;
						subtree_output_stored = if let Some(row) = row {
							let row =
								<SubtreeOutputStoredRow as db::sqlite::row::Deserialize>::deserialize(
									row,
								)
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to deserialize the subtree output stored flag"
									)
								})?;
							row.subtree_output_stored
						} else {
							false
						};

						// Update output stored.
						let params = [item.process.to_bytes().to_vec()];
						update_node_output_stored_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the update output stored statement"
								)
							})?;
					}

					if subtree_output_stored {
						let params =
							sqlite::params![item.process.to_bytes().to_vec(), item.transaction_id];
						enqueue_parents_output_statement
							.execute(params)
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to execute the enqueue parents statement"
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
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			#[tangram_database(try_from = "Vec<u8>")]
			cache_entry: tg::artifact::Id,
		}

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

		let ids = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Row as db::sqlite::row::Deserialize>::deserialize)
			.map(|result| {
				result.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map_ok(|row| row.cache_entry)
			.collect::<tg::Result<Vec<tg::artifact::Id>>>()?;
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
			reference_count_statement
				.execute([id.to_bytes().to_vec()])
				.map_err(|source| {
					tg::error!(!source, "failed to execute the reference count statement")
				})?;
		}

		Ok(ids.len())
	}

	fn indexer_handle_reference_count_object_sqlite(
		transaction: &sqlite::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Item {
			#[tangram_database(try_from = "Vec<u8>")]
			object: tg::object::Id,
		}

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

		let items = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Item as db::sqlite::row::Deserialize>::deserialize)
			.map(|row| row.map_err(|source| tg::error!(!source, "failed to deserialize the row")))
			.collect::<tg::Result<Vec<Item>>>()?;
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
			let params = [item.object.to_bytes().to_vec()];
			reference_count_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the reference count statement")
				})?;

			// Increment the children's reference counts.
			let params = [item.object.to_bytes().to_vec()];
			children_statement.execute(params).map_err(|source| {
				tg::error!(!source, "failed to execute the children statement")
			})?;

			// Update the cache entries' reference counts.
			let params = [item.object.to_bytes().to_vec()];
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
		#[derive(db::sqlite::row::Deserialize)]
		struct Item {
			#[tangram_database(try_from = "Vec<u8>")]
			process: tg::process::Id,
		}

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

		let items = statement
			.query([n])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Item as db::sqlite::row::Deserialize>::deserialize)
			.map(|row| row.map_err(|source| tg::error!(!source, "failed to deserialize the row")))
			.collect::<tg::Result<Vec<Item>>>()?;
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
				.execute([item.process.to_bytes().to_vec()])
				.map_err(|source| {
					tg::error!(!source, "failed to execute the reference count statement")
				})?;
			children_statement
				.execute([item.process.to_bytes().to_vec()])
				.map_err(|source| {
					tg::error!(!source, "failed to execute the children statement")
				})?;
			objects_statement
				.execute([item.process.to_bytes().to_vec()])
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

pub fn initialize(connection: &sqlite::Connection) -> sqlite::Result<()> {
	connection.pragma_update(None, "auto_vaccum", "incremental")?;
	connection.pragma_update(None, "busy_timeout", "5000")?;
	connection.pragma_update(None, "cache_size", "-20000")?;
	connection.pragma_update(None, "foreign_keys", "on")?;
	connection.pragma_update(None, "journal_mode", "wal")?;
	connection.pragma_update(None, "mmap_size", "2147483648")?;
	connection.pragma_update(None, "recursive_triggers", "on")?;
	connection.pragma_update(None, "synchronous", "off")?;
	connection.pragma_update(None, "temp_store", "memory")?;

	let function = |context: &sqlite::functions::Context| -> sqlite::Result<sqlite::types::Value> {
		let blob = context.get::<Vec<u8>>(0)?;
		let id = tg::Id::from_slice(&blob)
			.map_err(|source| sqlite::Error::UserFunctionError(source.into()))?;
		let text = sqlite::types::Value::Text(id.to_string());
		Ok(text)
	};
	let flags = sqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
		| sqlite::functions::FunctionFlags::SQLITE_UTF8;
	connection.create_scalar_function("id_blob_to_text", 1, flags, function)?;

	let function = |context: &sqlite::functions::Context| -> sqlite::Result<sqlite::types::Value> {
		let text = context.get::<String>(0)?;
		let id = tg::Id::from_str(&text)
			.map_err(|source| sqlite::Error::UserFunctionError(source.into()))?;
		let blob = sqlite::types::Value::Blob(id.to_bytes().to_vec());
		Ok(blob)
	};
	let flags = sqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
		| sqlite::functions::FunctionFlags::SQLITE_UTF8;
	connection.create_scalar_function("id_text_to_blob", 1, flags, function)?;

	Ok(())
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
	let sql = include_str!("./sqlite.sql");
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
