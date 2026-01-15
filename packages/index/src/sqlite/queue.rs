use {
	super::Index,
	indoc::indoc,
	itertools::Itertools as _,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Index {
	pub async fn handle_queue(&self, batch_size: usize) -> tg::Result<usize> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let n = connection
			.with(move |connection, cache| {
				// Begin a transaction.
				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				let mut n = batch_size;
				n -= Self::handle_stored_object(&transaction, cache, n)?;
				n -= Self::handle_stored_process(&transaction, cache, n)?;
				n -= Self::handle_reference_count_cache_entry(&transaction, cache, n)?;
				n -= Self::handle_reference_count_object(&transaction, cache, n)?;
				n -= Self::handle_reference_count_process(&transaction, cache, n)?;
				let n = batch_size - n;

				// Commit the transaction.
				if n > 0 {
					Self::increment_transaction_id(&transaction, cache)?;
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
				}

				Ok::<_, tg::Error>(n)
			})
			.await?;

		Ok(n)
	}

	fn handle_stored_object(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		n: usize,
	) -> tg::Result<usize> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Item {
			#[tangram_database(try_from = "Vec<u8>")]
			object: tg::object::Id,
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
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
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;

		let items = statement
			.query([n.to_i64().unwrap()])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Item as db::sqlite::row::Deserialize>::deserialize)
			.map(|result| {
				result.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.collect::<tg::Result<Vec<Item>>>()?;
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				insert into object_queue (object, kind, transaction_id)
				select object, 1, ?2
				from object_children
				where object_children.child = ?1
				on conflict (object, kind) do nothing;
			"
		);
		let mut enqueue_parents_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue parents statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 2, ?2
				from process_objects
				where process_objects.object = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_commands_processes_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue processes statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 3, ?2
				from process_objects
				where process_objects.object = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_errors_processes_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue processes statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 4, ?2
				from process_objects
				where process_objects.object = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_logs_processes_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue processes statement")
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 5, ?2
				from process_objects
				where process_objects.object = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_outputs_processes_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the enqueue processes statement")
			})?;

		let statement = indoc!(
			"
				update objects
				set
					subtree_stored = updates.subtree_stored,
					subtree_count = coalesce(objects.subtree_count, updates.subtree_count),
					subtree_depth = coalesce(objects.subtree_depth, updates.subtree_depth),
					subtree_size = coalesce(objects.subtree_size, updates.subtree_size),
					subtree_solvable = coalesce(objects.subtree_solvable, updates.subtree_solvable),
					subtree_solved = coalesce(objects.subtree_solved, updates.subtree_solved)
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
					objects.node_size + coalesce(sum(coalesce(child_objects.subtree_size, 0)), 0) as subtree_size,
					objects.node_solvable or coalesce(max(coalesce(child_objects.subtree_solvable, 0)), 0) as subtree_solvable,
					objects.node_solved and coalesce(min(coalesce(child_objects.subtree_solved, 1)), 1) as subtree_solved
					from objects
					left join object_children on object_children.object = objects.id
					left join objects as child_objects on child_objects.id = object_children.child
					where objects.id = ?1
					and (
						objects.subtree_stored = 0 or
						objects.subtree_count is null or
						objects.subtree_depth is null or
						objects.subtree_size is null or
						objects.subtree_solvable is null or
						objects.subtree_solved is null
					)
					group by objects.id, objects.node_size, objects.node_solvable, objects.node_solved
				) as updates
				where objects.id = updates.id
				and updates.subtree_stored = 1
				returning objects.subtree_stored;
			"
		);
		let mut update_subtree_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the update subtree stored statement"
				)
			})?;

		for item in &items {
			// Update the object.
			let params = [item.object.to_bytes().to_vec()];
			let mut rows = update_subtree_stored_statement
				.query(params)
				.map_err(|source| {
					tg::error!(
						!source,
						"failed to execute the update subtree stored statement"
					)
				})?;
			rows.next().map_err(|source| {
				tg::error!(
					!source,
					"failed to execute the update subtree stored statement"
				)
			})?;

			// Enqueue parents and processes.
			let params = sqlite::params![
				item.object.to_bytes().to_vec(),
				item.transaction_id.to_i64().unwrap()
			];
			enqueue_parents_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the enqueue parents statement")
				})?;

			let params = sqlite::params![
				item.object.to_bytes().to_vec(),
				item.transaction_id.to_i64().unwrap()
			];
			enqueue_commands_processes_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the enqueue processes statement")
				})?;

			let params = sqlite::params![
				item.object.to_bytes().to_vec(),
				item.transaction_id.to_i64().unwrap()
			];
			enqueue_errors_processes_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the enqueue processes statement")
				})?;

			let params = sqlite::params![
				item.object.to_bytes().to_vec(),
				item.transaction_id.to_i64().unwrap()
			];
			enqueue_logs_processes_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the enqueue processes statement")
				})?;

			let params = sqlite::params![
				item.object.to_bytes().to_vec(),
				item.transaction_id.to_i64().unwrap()
			];
			enqueue_outputs_processes_statement
				.execute(params)
				.map_err(|source| {
					tg::error!(!source, "failed to execute the enqueue processes statement")
				})?;
		}

		Ok(items.len())
	}

	fn handle_stored_process(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
		n: usize,
	) -> tg::Result<usize> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Item {
			#[tangram_database(try_from = "Vec<u8>")]
			process: tg::process::Id,
			#[tangram_database(try_from = "i64")]
			kind: Kind,
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			transaction_id: u64,
		}

		#[derive(derive_more::TryFrom)]
		#[try_from(repr)]
		#[repr(i64)]
		enum Kind {
			Children = 1,
			Commands = 2,
			Errors = 3,
			Logs = 4,
			Outputs = 5,
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
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;

		let items = statement
			.query([n.to_i64().unwrap()])
			.map_err(|source| tg::error!(!source, "failed to execute the dequeue statement"))?
			.and_then(<Item as db::sqlite::row::Deserialize>::deserialize)
			.map(|row| row.map_err(|source| tg::error!(!source, "failed to deserialize the row")))
			.collect::<tg::Result<Vec<Item>>>()?;
		if items.is_empty() {
			return Ok(0);
		}

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 1, ?2
				from process_children
				where process_children.child = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_parents_process_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
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
				where process_children.child = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_parents_command_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents command
				 statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 4, ?2
				from process_children
				where process_children.child = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_parents_log_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents log statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 5, ?2
				from process_children
				where process_children.child = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_parents_output_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents output statement"
				)
			})?;

		let statement = indoc!(
			"
				insert into process_queue (process, kind, transaction_id)
				select process, 3, ?2
				from process_children
				where process_children.child = ?1
				on conflict (process, kind) do nothing;
			"
		);
		let mut enqueue_parents_error_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the enqueue parents error statement"
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
					and (
						processes.subtree_stored = 0 or
						processes.subtree_count is null
					)
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.subtree_stored = 1
				returning subtree_stored;
			"
		);
		let mut update_subtree_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
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
					and (
						processes.node_command_stored = 0 or
						processes.node_command_count is null or
						processes.node_command_depth is null or
						processes.node_command_size is null
					);
			"
		);
		let mut update_node_command_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set
					node_log_stored = updates.node_log_stored,
					node_log_count = updates.node_log_count,
					node_log_depth = updates.node_log_depth,
					node_log_size = updates.node_log_size
				from (
					select
						processes.id,
						case
							when count(process_objects.object) = 0 then 1
							else min(coalesce(objects.subtree_stored, 0))
						end as node_log_stored,
						coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_log_count,
						coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_log_depth,
						coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_log_size
					from processes
					left join process_objects on process_objects.process = processes.id and process_objects.kind = 2
					left join objects on objects.id = process_objects.object
					where processes.id = ?1
					and (
						processes.node_log_stored = 0 or
						processes.node_log_count is null or
						processes.node_log_depth is null or
						processes.node_log_size is null
					)
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.node_log_stored = 1;
			"
		);
		let mut update_node_log_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
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
						processes.id,
						case
							when count(process_objects.object) = 0 then 1
							else min(coalesce(objects.subtree_stored, 0))
						end as node_output_stored,
						coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_output_count,
						coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_output_depth,
						coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_output_size
					from processes
					left join process_objects on process_objects.process = processes.id and process_objects.kind = 3
					left join objects on objects.id = process_objects.object
					where processes.id = ?1
					and (
						processes.node_output_stored = 0 or
						processes.node_output_count is null or
						processes.node_output_depth is null or
						processes.node_output_size is null
					)
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.node_output_stored = 1;
			"
		);
		let mut update_node_output_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set
					node_error_stored = updates.node_error_stored,
					node_error_count = updates.node_error_count,
					node_error_depth = updates.node_error_depth,
					node_error_size = updates.node_error_size
				from (
					select
						processes.id,
						case
							when count(process_objects.object) = 0 then 1
							else min(coalesce(objects.subtree_stored, 0))
						end as node_error_stored,
						coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_error_count,
						coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_error_depth,
						coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_error_size
					from processes
					left join process_objects on process_objects.process = processes.id and process_objects.kind = 1
					left join objects on objects.id = process_objects.object
					where processes.id = ?1
					and (
						processes.node_error_stored = 0 or
						processes.node_error_count is null or
						processes.node_error_depth is null or
						processes.node_error_size is null
					)
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.node_error_stored = 1;
			"
		);
		let mut update_node_error_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
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
								coalesce(command_objects.subtree_stored, 0)
								and (coalesce(child_processes.child_count, 0) = 0 or child_processes.all_stored)
								then 1
							else 0
						end as subtree_command_stored,
						coalesce(command_objects.subtree_count, 0) + coalesce(child_processes.subtree_command_count, 0) as subtree_command_count,
						max(coalesce(command_objects.subtree_depth, 0), coalesce(child_processes.subtree_command_depth, 0)) as subtree_command_depth,
						coalesce(command_objects.subtree_size, 0) + coalesce(child_processes.subtree_command_size, 0) as subtree_command_size
					from processes
					left join (
						select
							process_objects.process,
							objects.subtree_stored,
							objects.subtree_count,
							objects.subtree_depth,
							objects.subtree_size
						from process_objects
						left join objects on objects.id = process_objects.object
						where process_objects.kind = 0
					) as command_objects on command_objects.process = processes.id
					left join (
						select
							process_children.process,
							count(process_children.child) as child_count,
							min(coalesce(child.subtree_command_stored, 0)) as all_stored,
							sum(coalesce(child.subtree_command_count, 0)) as subtree_command_count,
							max(coalesce(child.subtree_command_depth, 0)) as subtree_command_depth,
							sum(coalesce(child.subtree_command_size, 0)) as subtree_command_size
						from process_children
						left join processes child on child.id = process_children.child
						group by process_children.process
					) as child_processes on child_processes.process = processes.id
					where processes.id = ?1
					and (
						processes.subtree_command_stored = 0
						or processes.subtree_command_count is null
						or processes.subtree_command_depth is null
						or processes.subtree_command_size is null
					)
				) as updates
				where processes.id = updates.id
				and updates.subtree_command_stored = 1
				returning subtree_command_stored;
			"
		);
		let mut update_subtree_command_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
					set
						subtree_log_stored = updates.subtree_log_stored,
						subtree_log_count = updates.subtree_log_count,
						subtree_log_depth = updates.subtree_log_depth,
						subtree_log_size = updates.subtree_log_size
				from (
					select
						processes.id,
						case
							when count(process_children.child) = 0
							and (count(process_objects.object) = 0 or min(coalesce(objects.subtree_stored, 0)) = 1)
								then 1
							when (count(process_objects.object) = 0 or min(coalesce(objects.subtree_stored, 0)) = 1)
							and (count(process_children.child) = 0 or min(coalesce(child_processes.subtree_log_stored, 0)) = 1)
								then 1
							else 0
						end as subtree_log_stored,
						coalesce(sum(coalesce(objects.subtree_count, 0)), 0)
						+ coalesce(sum(coalesce(child_processes.subtree_log_count, 0)), 0) as subtree_log_count,
						max(
						coalesce(max(coalesce(objects.subtree_depth, 0)), 0),
						coalesce(max(coalesce(child_processes.subtree_log_depth, 0)), 0)
						) as subtree_log_depth,
						coalesce(sum(coalesce(objects.subtree_size, 0)), 0)
						+ coalesce(sum(coalesce(child_processes.subtree_log_size, 0)), 0) as subtree_log_size
					from processes
					left join process_objects on process_objects.process = processes.id and process_objects.kind = 2
					left join objects on objects.id = process_objects.object
					left join process_children on process_children.process = processes.id
					left join processes child_processes on child_processes.id = process_children.child
					where processes.id = ?1
					and (
						processes.subtree_log_stored = 0 or
						processes.subtree_log_count is null or
						processes.subtree_log_depth is null or
						processes.subtree_log_size is null
					)
					group by processes.id
				) as updates
				where processes.id = updates.id
				and updates.subtree_log_stored = 1
				returning subtree_log_stored;
			"
		);
		let mut update_subtree_log_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
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
								(output_objects.process is null or coalesce(output_objects.subtree_stored, 0))
								and (coalesce(child_processes.child_count, 0) = 0 or child_processes.all_stored)
								then 1
							else 0
						end as subtree_output_stored,
						coalesce(output_objects.subtree_count, 0) + coalesce(child_processes.subtree_output_count, 0) as subtree_output_count,
						max(coalesce(output_objects.subtree_depth, 0), coalesce(child_processes.subtree_output_depth, 0)) as subtree_output_depth,
						coalesce(output_objects.subtree_size, 0) + coalesce(child_processes.subtree_output_size, 0) as subtree_output_size
					from processes
					left join (
						select
							process_objects.process,
							objects.subtree_stored,
							objects.subtree_count,
							objects.subtree_depth,
							objects.subtree_size
						from process_objects
						left join objects on objects.id = process_objects.object
						where process_objects.kind = 3
					) as output_objects on output_objects.process = processes.id
					left join (
						select
							process_children.process,
							count(process_children.child) as child_count,
							min(coalesce(child.subtree_output_stored, 0)) as all_stored,
							sum(coalesce(child.subtree_output_count, 0)) as subtree_output_count,
							max(coalesce(child.subtree_output_depth, 0)) as subtree_output_depth,
							sum(coalesce(child.subtree_output_size, 0)) as subtree_output_size
						from process_children
						left join processes child on child.id = process_children.child
						group by process_children.process
					) as child_processes on child_processes.process = processes.id
					where processes.id = ?1
					and (
						processes.subtree_output_stored = 0
						or processes.subtree_output_count is null
						or processes.subtree_output_depth is null
						or processes.subtree_output_size is null
					)
				) as updates
				where processes.id = updates.id
				and updates.subtree_output_stored = 1
				returning subtree_output_stored;
			"
		);
		let mut update_subtree_output_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		let statement = indoc!(
			"
				update processes
				set
					subtree_error_stored = updates.subtree_error_stored,
					subtree_error_count = updates.subtree_error_count,
					subtree_error_depth = updates.subtree_error_depth,
					subtree_error_size = updates.subtree_error_size
				from (
					select
						processes.id,
						case
							when
								(error_objects.process is null or coalesce(error_objects.subtree_stored, 0))
								and (coalesce(child_processes.child_count, 0) = 0 or child_processes.all_stored)
								then 1
							else 0
						end as subtree_error_stored,
						coalesce(error_objects.subtree_count, 0) + coalesce(child_processes.subtree_error_count, 0) as subtree_error_count,
						max(coalesce(error_objects.subtree_depth, 0), coalesce(child_processes.subtree_error_depth, 0)) as subtree_error_depth,
						coalesce(error_objects.subtree_size, 0) + coalesce(child_processes.subtree_error_size, 0) as subtree_error_size
					from processes
					left join (
						select
							process_objects.process,
							objects.subtree_stored,
							objects.subtree_count,
							objects.subtree_depth,
							objects.subtree_size
						from process_objects
						left join objects on objects.id = process_objects.object
						where process_objects.kind = 1
					) as error_objects on error_objects.process = processes.id
					left join (
						select
							process_children.process,
							count(process_children.child) as child_count,
							min(coalesce(child.subtree_error_stored, 0)) as all_stored,
							sum(coalesce(child.subtree_error_count, 0)) as subtree_error_count,
							max(coalesce(child.subtree_error_depth, 0)) as subtree_error_depth,
							sum(coalesce(child.subtree_error_size, 0)) as subtree_error_size
						from process_children
						left join processes child on child.id = process_children.child
						group by process_children.process
					) as child_processes on child_processes.process = processes.id
					where processes.id = ?1
					and (
						processes.subtree_error_stored = 0
						or processes.subtree_error_count is null
						or processes.subtree_error_depth is null
						or processes.subtree_error_size is null
					)
				) as updates
				where processes.id = updates.id
				and updates.subtree_error_stored = 1
				returning subtree_error_stored;
			"
		);
		let mut update_subtree_error_stored_statement =
			cache.get(transaction, statement.into()).map_err(|source| {
				tg::error!(!source, "failed to prepare the update stored statement")
			})?;

		for item in &items {
			match item.kind {
				Kind::Children => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows =
						update_subtree_stored_statement
							.query(params)
							.map_err(|source| {
								tg::error!(!source, "failed to execute the update stored statement")
							})?;
					rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the update stored statement")
					})?;

					let params = sqlite::params![
						item.process.to_bytes().to_vec(),
						item.transaction_id.to_i64().unwrap()
					];
					enqueue_parents_process_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the enqueue parents statement")
						})?;
				},

				Kind::Commands => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows = update_subtree_command_stored_statement
						.query(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the update stored statement")
						})?;
					rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the update stored statement")
					})?;

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

					let params = sqlite::params![
						item.process.to_bytes().to_vec(),
						item.transaction_id.to_i64().unwrap()
					];
					enqueue_parents_command_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the enqueue parents statement")
						})?;
				},

				Kind::Errors => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows = update_subtree_error_stored_statement
						.query(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the update stored statement")
						})?;
					rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the update stored statement")
					})?;

					// Update error stored.
					let params = [item.process.to_bytes().to_vec()];
					update_node_error_stored_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(
								!source,
								"failed to execute the update error stored statement"
							)
						})?;

					let params = sqlite::params![
						item.process.to_bytes().to_vec(),
						item.transaction_id.to_i64().unwrap()
					];
					enqueue_parents_error_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the enqueue parents statement")
						})?;
				},

				Kind::Logs => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows =
						update_subtree_log_stored_statement
							.query(params)
							.map_err(|source| {
								tg::error!(!source, "failed to execute the update stored statement")
							})?;
					rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the update stored statement")
					})?;

					// Update log stored.
					let params = [item.process.to_bytes().to_vec()];
					update_node_log_stored_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the update log stored statement")
						})?;

					let params = sqlite::params![
						item.process.to_bytes().to_vec(),
						item.transaction_id.to_i64().unwrap()
					];
					enqueue_parents_log_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the enqueue parents statement")
						})?;
				},

				Kind::Outputs => {
					let params = [item.process.to_bytes().to_vec()];
					let mut rows = update_subtree_output_stored_statement
						.query(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the update stored statement")
						})?;
					rows.next().map_err(|source| {
						tg::error!(!source, "failed to execute the update stored statement")
					})?;

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

					let params = sqlite::params![
						item.process.to_bytes().to_vec(),
						item.transaction_id.to_i64().unwrap()
					];
					enqueue_parents_output_statement
						.execute(params)
						.map_err(|source| {
							tg::error!(!source, "failed to execute the enqueue parents statement")
						})?;
				},
			}
		}

		Ok(items.len())
	}

	fn handle_reference_count_cache_entry(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
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
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;

		let ids = statement
			.query([n.to_i64().unwrap()])
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
			cache.get(transaction, statement.into()).map_err(|source| {
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

	fn handle_reference_count_object(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
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
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;

		let items = statement
			.query([n.to_i64().unwrap()])
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
			cache.get(transaction, statement.into()).map_err(|source| {
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
		let mut children_statement = cache
			.get(transaction, statement.into())
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
			cache.get(transaction, statement.into()).map_err(|source| {
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

	fn handle_reference_count_process(
		transaction: &sqlite::Transaction<'_>,
		cache: &db::sqlite::Cache,
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
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the dequeue statement"))?;

		let items = statement
			.query([n.to_i64().unwrap()])
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
			cache.get(transaction, statement.into()).map_err(|source| {
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
		let mut children_statement = cache
			.get(transaction, statement.into())
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
		let mut objects_statement = cache
			.get(transaction, statement.into())
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
}
