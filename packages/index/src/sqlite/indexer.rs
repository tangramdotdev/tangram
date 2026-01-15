use {
	super::Index,
	crate::{
		DeleteTagArg, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg, TouchObjectArg,
		TouchProcessArg,
	},
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Index {
	#[expect(clippy::too_many_arguments)]
	pub async fn handle_messages(
		&self,
		put_cache_entry_messages: Vec<PutCacheEntryArg>,
		put_object_messages: Vec<PutObjectArg>,
		touch_object_messages: Vec<TouchObjectArg>,
		put_process_messages: Vec<PutProcessArg>,
		touch_process_messages: Vec<TouchProcessArg>,
		put_tag_messages: Vec<PutTagArg>,
		delete_tag_messages: Vec<DeleteTagArg>,
	) -> tg::Result<()> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with(move |connection, cache| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				// Handle the messages.
				Self::put_cache_entries(&transaction, cache, put_cache_entry_messages)?;
				Self::put_objects(&transaction, cache, put_object_messages)?;
				Self::touch_objects(&transaction, cache, touch_object_messages)?;
				Self::put_processes(&transaction, cache, put_process_messages)?;
				Self::touch_processes(&transaction, cache, touch_process_messages)?;
				Self::put_tags(&transaction, cache, put_tag_messages)?;
				Self::delete_tags(&transaction, cache, delete_tag_messages)?;
				Self::increment_transaction_id(&transaction, cache)?;

				// Commit the transaction.
				transaction
					.commit()
					.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

				Ok::<_, tg::Error>(())
			})
			.await?;

		Ok(())
	}

	fn put_cache_entries(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<PutCacheEntryArg>,
	) -> tg::Result<()> {
		let insert_statement = indoc!(
			"
				insert into cache_entries (id, touched_at)
				values (?1, ?2)
				on conflict (id) do nothing;
			"
		);
		let mut insert_statement = cache
			.get(transaction, insert_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let update_statement = indoc!(
			"
				update cache_entries
				set touched_at = max(touched_at, ?2)
				where id = ?1;
			"
		);
		let mut update_statement = cache
			.get(transaction, update_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let queue_statement = indoc!(
			"
				insert into cache_entry_queue (cache_entry, transaction_id)
				values (?1, (select id from transaction_id))
				on conflict (cache_entry) do nothing;
			"
		);
		let mut queue_statement = cache
			.get(transaction, queue_statement.into())
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

			// Enqueue.
			if inserted {
				let params = sqlite::params![message.id.to_bytes().to_vec()];
				queue_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}

	fn put_objects(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<PutObjectArg>,
	) -> tg::Result<()> {
		// Prepare insert statement for objects.
		let insert_statement = indoc!(
			"
				insert into objects (id, cache_entry, node_size, node_solvable, node_solved, subtree_count, subtree_depth, subtree_size, subtree_solvable, subtree_solved, subtree_stored, touched_at, transaction_id)
				values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, (select id from transaction_id))
				on conflict (id) do nothing;
			"
		);
		let mut insert_statement = cache
			.get(transaction, insert_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare query statement.
		#[derive(db::sqlite::row::Deserialize, Clone, PartialEq)]
		#[expect(clippy::struct_field_names)]
		struct Row {
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_depth: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_size: Option<u64>,
			subtree_stored: bool,
			subtree_solved: Option<bool>,
			subtree_solvable: Option<bool>,
		}
		let statement = indoc!(
			"
				select
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_stored,
					subtree_solved,
					subtree_solvable
				from objects
				where id = ?1;
			"
		);
		let mut subtree_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the subtree statement"))?;

		// Prepare update statement for objects.
		let update_statement = indoc!(
			"
				update objects
				set
					subtree_count = coalesce(subtree_count, ?6),
					subtree_depth = coalesce(subtree_depth, ?7),
					subtree_size = coalesce(subtree_size, ?8),
					subtree_solvable = coalesce(subtree_solvable, ?9),
					subtree_solved = coalesce(subtree_solved, ?10),
					subtree_stored = subtree_stored or ?11,
					touched_at = max(touched_at, ?12)
				where id = ?1
				returning
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solved,
					subtree_solvable,
					subtree_stored;
			"
		);
		let mut update_statement = cache
			.get(transaction, update_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare a statement for the object children.
		let children_statement = indoc!(
			"
				insert into object_children (object, child)
				values (?1, ?2)
				on conflict (object, child) do nothing;
			"
		);
		let mut children_statement = cache
			.get(transaction, children_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare statement for object queue.
		let queue_statement = indoc!(
			"
				insert into object_queue (object, kind, transaction_id)
				values (?1, ?2, (select id from transaction_id))
				on conflict (object, kind) do nothing;
			"
		);
		let mut queue_statement = cache
			.get(transaction, queue_statement.into())
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
			let node_size = metadata.node.size;
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
				node_size.to_i64().unwrap(),
				metadata.node.solvable,
				metadata.node.solved,
				metadata.subtree.count.map(|v| v.to_i64().unwrap()),
				metadata.subtree.depth.map(|v| v.to_i64().unwrap()),
				metadata.subtree.size.map(|v| v.to_i64().unwrap()),
				metadata.subtree.solvable,
				metadata.subtree.solved,
				stored.subtree,
				touched_at,
			];
			let rows = insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let inserted = rows > 0;

			// If not inserted, update instead.
			let mut changed = inserted;
			if !inserted {
				// Get the old values.
				let params = [id.to_bytes().to_vec()];
				let mut rows = subtree_statement
					.query(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let old_row = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
					.ok_or_else(|| tg::error!("expected a row"))?;
				let old_row = <Row as db::sqlite::row::Deserialize>::deserialize(old_row)
					.map_err(|source| tg::error!(!source, "failed to deserialize"))?;

				// Update and get the new values.
				let params = sqlite::params![
					&id.to_bytes().to_vec(),
					cache_entry,
					node_size.to_i64().unwrap(),
					metadata.node.solvable,
					metadata.node.solved,
					metadata.subtree.count.map(|v| v.to_i64().unwrap()),
					metadata.subtree.depth.map(|v| v.to_i64().unwrap()),
					metadata.subtree.size.map(|v| v.to_i64().unwrap()),
					metadata.subtree.solvable,
					metadata.subtree.solved,
					stored.subtree,
					touched_at,
				];
				let mut rows = update_statement
					.query(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let row = rows
					.next()
					.map_err(|source| {
						tg::error!(!source, "failed to execute the update subtree statement")
					})?
					.ok_or_else(|| tg::error!("expected a row"))?;
				let new_row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize row"))?;

				changed = old_row != new_row;
			}

			if inserted {
				// Enqueue for reference count.
				let params = sqlite::params![&id.to_bytes().to_vec(), 0];
				queue_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Newly inserted rows always enqueue parents. Updated rows only enqueue parents if one of their subtree fields changed.
			if changed {
				// Enqueue for stored and metadata.
				let params = sqlite::params![&id.to_bytes().to_vec(), 1];
				queue_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}
		}

		Ok(())
	}

	fn touch_objects(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<TouchObjectArg>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update objects
				set touched_at = max(touched_at, ?1)
				where id = ?2;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.touched_at, message.id.to_bytes().to_vec()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn put_processes(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<PutProcessArg>,
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
					node_error_count,
					node_error_depth,
					node_error_size,
					node_error_stored,
					node_log_count,
					node_log_depth,
					node_log_size,
					node_log_stored,
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_count,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_error_stored,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_log_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
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
					?21,
					?22,
					?23,
					?24,
					?25,
					?26,
					?27,
					?28,
					?29,
					?30,
					?31,
					?32,
					?33,
					?34,
					?35,
					?36,
					(select id from transaction_id)
				)
				on conflict (id) do nothing;
			"
		);
		let mut insert_statement = cache
			.get(transaction, insert_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare query statement for processes.
		#[derive(db::sqlite::row::Deserialize, Clone, PartialEq)]
		#[expect(clippy::struct_field_names)]
		struct Row {
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_command_size: Option<u64>,
			subtree_command_stored: bool,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_error_count: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_error_depth: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_error_size: Option<u64>,
			subtree_error_stored: bool,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_log_count: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_log_depth: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_log_size: Option<u64>,
			subtree_log_stored: bool,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
			subtree_output_size: Option<u64>,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_count,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_error_stored,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_log_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_stored
				from processes
				where id = ?1;
			"
		);
		let mut query_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the query statement"))?;

		// Prepare update statement for processes.
		let update_statement = indoc!(
			"
				update processes
				set
					node_command_count = coalesce(node_command_count, ?2),
					node_command_depth = coalesce(node_command_depth, ?3),
					node_command_size = coalesce(node_command_size, ?4),
					node_command_stored = node_command_stored or ?5,
					node_error_count = coalesce(node_error_count, ?6),
					node_error_depth = coalesce(node_error_depth, ?7),
					node_error_size = coalesce(node_error_size, ?8),
					node_error_stored = node_error_stored or ?9,
					node_log_count = coalesce(node_log_count, ?10),
					node_log_depth = coalesce(node_log_depth, ?11),
					node_log_size = coalesce(node_log_size, ?12),
					node_log_stored = node_log_stored or ?13,
					node_output_count = coalesce(node_output_count, ?14),
					node_output_depth = coalesce(node_output_depth, ?15),
					node_output_size = coalesce(node_output_size, ?16),
					node_output_stored = node_output_stored or ?17,
					subtree_command_count = coalesce(subtree_command_count, ?18),
					subtree_command_depth = coalesce(subtree_command_depth, ?19),
					subtree_command_size = coalesce(subtree_command_size, ?20),
					subtree_command_stored = subtree_command_stored or ?21,
					subtree_count = coalesce(subtree_count, ?22),
					subtree_error_count = coalesce(subtree_error_count, ?23),
					subtree_error_depth = coalesce(subtree_error_depth, ?24),
					subtree_error_size = coalesce(subtree_error_size, ?25),
					subtree_error_stored = subtree_error_stored or ?26,
					subtree_log_count = coalesce(subtree_log_count, ?27),
					subtree_log_depth = coalesce(subtree_log_depth, ?28),
					subtree_log_size = coalesce(subtree_log_size, ?29),
					subtree_log_stored = subtree_log_stored or ?30,
					subtree_output_count = coalesce(subtree_output_count, ?31),
					subtree_output_depth = coalesce(subtree_output_depth, ?32),
					subtree_output_size = coalesce(subtree_output_size, ?33),
					subtree_output_stored = subtree_output_stored or ?34,
					subtree_stored = subtree_stored or ?35,
					touched_at = max(touched_at, ?36)
				where id = ?1
				returning
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_count,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_error_stored,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_log_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_stored;
			"
		);
		let mut update_statement = cache
			.get(transaction, update_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let object_statement = indoc!(
			"
				insert into process_objects (process, object, kind)
				values (?1, ?2, ?3)
				on conflict (process, object, kind) do nothing;
			"
		);
		let mut object_statement = cache
			.get(transaction, object_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let child_statement = indoc!(
			"
				insert into process_children (process, position, child)
				values (?1, ?2, ?3)
				on conflict (process, child) do nothing;
			"
		);
		let mut child_statement = cache
			.get(transaction, child_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		// Prepare statement for process queue.
		let queue_statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				values (?1, ?2, (select id from transaction_id))
				on conflict (process, kind) do nothing;
			"
		);
		let mut queue_statement = cache
			.get(transaction, queue_statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		for message in messages {
			// Try to insert the process.
			let params = sqlite::params![
				message.id.to_bytes().to_vec(),
				message
					.metadata
					.node
					.command
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.node
					.command
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.node
					.command
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.node_command,
				message
					.metadata
					.node
					.error
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.node
					.error
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.node
					.error
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.node_error,
				message.metadata.node.log.count.map(|v| v.to_i64().unwrap()),
				message.metadata.node.log.depth.map(|v| v.to_i64().unwrap()),
				message.metadata.node.log.size.map(|v| v.to_i64().unwrap()),
				message.stored.node_log,
				message
					.metadata
					.node
					.output
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.node
					.output
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.node
					.output
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.node_output,
				message
					.metadata
					.subtree
					.command
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.command
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.command
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.subtree_command,
				message.metadata.subtree.count.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.error
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.error
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.error
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.subtree_error,
				message
					.metadata
					.subtree
					.log
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.log
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.log
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.subtree_log,
				message
					.metadata
					.subtree
					.output
					.count
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.output
					.depth
					.map(|v| v.to_i64().unwrap()),
				message
					.metadata
					.subtree
					.output
					.size
					.map(|v| v.to_i64().unwrap()),
				message.stored.subtree_output,
				message.stored.subtree,
				message.touched_at,
			];
			let rows = insert_statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let inserted = rows > 0;

			// If not inserted, update instead.
			let mut changed = inserted;
			if !inserted {
				// Get the old values.
				let params = [message.id.to_bytes().to_vec()];
				let mut rows = query_statement.query(params).map_err(|source| {
					tg::error!(!source, "failed to execute the query statement")
				})?;
				let old_row = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to get next row"))?
					.ok_or_else(|| tg::error!("expected a row"))?;
				let old_row = <Row as db::sqlite::row::Deserialize>::deserialize(old_row)
					.map_err(|source| tg::error!(!source, "failed to deserialize row"))?;
				drop(rows);

				// Update and get the new values.
				let params = sqlite::params![
					message.id.to_bytes().to_vec(),
					message
						.metadata
						.node
						.command
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.node
						.command
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.node
						.command
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.node_command,
					message
						.metadata
						.node
						.error
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.node
						.error
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.node
						.error
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.node_error,
					message.metadata.node.log.count.map(|v| v.to_i64().unwrap()),
					message.metadata.node.log.depth.map(|v| v.to_i64().unwrap()),
					message.metadata.node.log.size.map(|v| v.to_i64().unwrap()),
					message.stored.node_log,
					message
						.metadata
						.node
						.output
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.node
						.output
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.node
						.output
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.node_output,
					message
						.metadata
						.subtree
						.command
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.command
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.command
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.subtree_command,
					message.metadata.subtree.count.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.error
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.error
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.error
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.subtree_error,
					message
						.metadata
						.subtree
						.log
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.log
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.log
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.subtree_log,
					message
						.metadata
						.subtree
						.output
						.count
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.output
						.depth
						.map(|v| v.to_i64().unwrap()),
					message
						.metadata
						.subtree
						.output
						.size
						.map(|v| v.to_i64().unwrap()),
					message.stored.subtree_output,
					message.stored.subtree,
					message.touched_at,
				];
				let mut rows = update_statement.query(params).map_err(|source| {
					tg::error!(!source, "failed to execute the update statement")
				})?;
				let row = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to get next row"))?
					.ok_or_else(|| tg::error!("expected a row"))?;
				let new_row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize row"))?;
				drop(rows);

				changed = old_row != new_row;
			}

			// Insert the children.
			for (position, child) in message.children.iter().enumerate() {
				let params = sqlite::params![
					message.id.to_bytes().to_vec(),
					position.to_i64().unwrap(),
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

			if inserted {
				// Enqueue for reference count.
				let params = sqlite::params![message.id.to_bytes().to_vec(), 0];
				queue_statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Newly inserted rows always enqueue parents. Updated rows only enqueue parents if one of their subtree fields changed.
			if changed {
				for kind in [1, 2, 3, 4, 5] {
					let params = sqlite::params![message.id.to_bytes().to_vec(), kind];
					queue_statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
			}
		}

		Ok(())
	}

	fn touch_processes(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<TouchProcessArg>,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				update processes
				set touched_at = max(touched_at, ?1)
				where id = ?2;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		for message in messages {
			let params = sqlite::params![message.touched_at, message.id.to_bytes().to_vec()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	fn put_tags(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<PutTagArg>,
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
		let mut get_old_item_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				insert or replace into tags (tag, item)
				values (?1, ?2);
			"
		);
		let mut insert_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut objects_increment_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut processes_increment_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count + 1
				where id = ?1
			"
		);
		let mut cache_entries_increment_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut objects_decrement_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut processes_decrement_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut cache_entries_decrement_reference_count_statement = cache
			.get(transaction, statement.into())
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
				tg::Either::Left(item) => item.to_bytes().to_vec(),
				tg::Either::Right(item) => item.to_bytes().to_vec(),
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

	fn delete_tags(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		messages: Vec<DeleteTagArg>,
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
		let mut delete_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut update_object_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut update_process_reference_count_statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count - 1
				where id = ?1;
			"
		);
		let mut update_cache_entry_reference_count_statement = cache
			.get(transaction, statement.into())
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
}
