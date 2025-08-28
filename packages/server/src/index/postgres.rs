use super::message::{
	DeleteTag, PutCacheEntry, PutObject, PutProcess, PutTagMessage, TouchObject, TouchProcess,
};
use crate::Server;
use indoc::indoc;
use num::ToPrimitive as _;
use std::collections::HashMap;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;

impl Server {
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn indexer_task_handle_messages_postgres(
		&self,
		database: &db::postgres::Database,
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
		let mut connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Handle the messages.
		self.indexer_put_cache_entries_postgres(put_cache_entry_messages, &transaction)
			.await?;
		self.indexer_put_objects_postgres(put_object_messages, &transaction)
			.await?;
		self.indexer_touch_objects_postgres(touch_object_messages, &transaction)
			.await?;
		self.indexer_put_processes_postgres(put_process_messages, &transaction)
			.await?;
		self.indexer_touch_processes_postgres(touch_process_messages, &transaction)
			.await?;
		self.indexer_put_tags_postgres(put_tag_messages, &transaction)
			.await?;
		self.indexer_delete_tags_postgres(delete_tag_messages, &transaction)
			.await?;
		self.indexer_increment_transaction_id_postgres(&transaction)
			.await?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	async fn indexer_put_cache_entries_postgres(
		&self,
		messages: Vec<PutCacheEntry>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let cache_entry_ids = messages
			.iter()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let touched_ats = messages
			.iter()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				insert into cache_entries (id, touched_at)
				select id, touched_at
				from unnest($1::text[], $2::int8[]) as t (id, touched_at)
				on conflict (id) do update set touched_at = excluded.touched_at;
			"
		);
		let params = db::params![cache_entry_ids.as_slice(), touched_ats.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	async fn indexer_put_objects_postgres(
		&self,
		messages: Vec<PutObject>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		let messages: HashMap<&tg::object::Id, &PutObject, fnv::FnvBuildHasher> = messages
			.iter()
			.map(|message| (&message.id, message))
			.collect();
		if messages.is_empty() {
			return Ok(());
		}
		let ids = messages
			.values()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let size = messages
			.values()
			.map(|message| message.size.to_i64().unwrap())
			.collect::<Vec<_>>();
		let touched_ats = messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let counts = messages
			.values()
			.map(|message| message.metadata.count.map(|count| count.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let depths = messages
			.values()
			.map(|message| message.metadata.depth.map(|depth| depth.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let weights = messages
			.values()
			.map(|message| {
				message
					.metadata
					.weight
					.map(|weight| weight.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let completes = messages
			.values()
			.map(|message| message.complete)
			.collect::<Vec<_>>();
		let children = messages
			.values()
			.flat_map(|message| message.children.iter().map(ToString::to_string))
			.collect::<Vec<_>>();
		let parent_indices = messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
			})
			.collect::<Vec<_>>();
		let cache_entries = messages
			.values()
			.filter_map(|message| message.cache_entry.as_ref())
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				call insert_objects(
					$1::text[],
					$2::text[],
					$3::int8[],
					$4::int8[],
					$5::int8[],
					$6::int8[],
					$7::int8[],
					$8::bool[],
					$9::text[],
					$10::int8[]
				);
			"
		);
		transaction
			.inner()
			.execute(
				statement,
				&[
					&ids.as_slice(),
					&cache_entries.as_slice(),
					&size.as_slice(),
					&touched_ats.as_slice(),
					&counts.as_slice(),
					&depths.as_slice(),
					&weights.as_slice(),
					&completes.as_slice(),
					&children.as_slice(),
					&parent_indices.as_slice(),
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(())
	}

	async fn indexer_touch_objects_postgres(
		&self,
		messages: Vec<TouchObject>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let touched_ats = messages
			.iter()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let object_ids = messages
			.iter()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				update objects
				set touched_at = t.touched_at
				from unnest($1::int8[], $2::text[]) as t (touched_at, id)
				where objects.id = t.id;
			"
		);
		let params = db::params![touched_ats.as_slice(), object_ids.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	async fn indexer_put_processes_postgres(
		&self,
		messages: Vec<PutProcess>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		let messages: HashMap<&tg::process::Id, &PutProcess, fnv::FnvBuildHasher> = messages
			.iter()
			.map(|message| (&message.id, message))
			.collect();
		if messages.is_empty() {
			return Ok(());
		}
		let process_ids = messages
			.values()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let touched_ats = messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let children_complete = messages
			.values()
			.map(|message| message.complete.children)
			.collect::<Vec<_>>();
		let children_counts = messages
			.values()
			.map(|message| message.metadata.count.map(|value| value.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let commands_complete = messages
			.values()
			.map(|message| message.complete.commands)
			.collect::<Vec<_>>();
		let commands_counts = messages
			.values()
			.map(|message| {
				message
					.metadata
					.commands
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let commands_depths = messages
			.values()
			.map(|message| {
				message
					.metadata
					.commands
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let commands_weights = messages
			.values()
			.map(|message| {
				message
					.metadata
					.commands
					.weight
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let outputs_complete = messages
			.values()
			.map(|message| message.complete.outputs)
			.collect::<Vec<_>>();
		let outputs_counts = messages
			.values()
			.map(|message| {
				message
					.metadata
					.outputs
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let outputs_depths = messages
			.values()
			.map(|message| {
				message
					.metadata
					.outputs
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let outputs_weights = messages
			.values()
			.map(|message| {
				message
					.metadata
					.outputs
					.weight
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let children = messages
			.values()
			.flat_map(|message| message.children.iter().map(ToString::to_string))
			.collect::<Vec<_>>();
		let child_process_indices = messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
			})
			.collect::<Vec<_>>();
		let child_positions = messages
			.values()
			.flat_map(|message| (0..message.children.len().to_i64().unwrap()).collect::<Vec<_>>())
			.collect::<Vec<_>>();
		let objects = messages
			.values()
			.flat_map(|message| message.objects.iter().map(|(id, _)| id.to_string()))
			.collect::<Vec<_>>();
		let object_kinds = messages
			.values()
			.flat_map(|message| message.objects.iter().map(|(_, kind)| kind.to_string()))
			.collect::<Vec<_>>();
		let object_process_indices = messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.objects.len())
			})
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				call insert_processes(
					$1::text[],
					$2::int8[],
					$3::bool[],
					$4::int8[],
					$5::bool[],
					$6::int8[],
					$7::int8[],
					$8::int8[],
					$9::bool[],
					$10::int8[],
					$11::int8[],
					$12::int8[],
					$13::text[],
					$14::int8[],
					$15::int8[],
					$16::text[],
					$17::text[],
					$18::int8[]
				);
			"
		);
		transaction
			.inner()
			.execute(
				statement,
				&[
					&process_ids.as_slice(),
					&touched_ats.as_slice(),
					&children_complete.as_slice(),
					&children_counts.as_slice(),
					&commands_complete.as_slice(),
					&commands_counts.as_slice(),
					&commands_depths.as_slice(),
					&commands_weights.as_slice(),
					&outputs_complete.as_slice(),
					&outputs_counts.as_slice(),
					&outputs_depths.as_slice(),
					&outputs_weights.as_slice(),
					&children.as_slice(),
					&child_process_indices.as_slice(),
					&child_positions.as_slice(),
					&objects.as_slice(),
					&object_kinds.as_slice(),
					&object_process_indices.as_slice(),
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(())
	}

	async fn indexer_touch_processes_postgres(
		&self,
		messages: Vec<TouchProcess>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let touched_ats = messages
			.iter()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let process_ids = messages
			.iter()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				update processes
				set touched_at = t.touched_at
				from unnest($1::int8[], $2::text[]) as t (touched_at, id)
				where processes.id = t.id;
			"
		);
		let params = db::params![touched_ats.as_slice(), process_ids.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	async fn indexer_put_tags_postgres(
		&self,
		messages: Vec<PutTagMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let tags = messages
			.iter()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();
		let items = messages
			.iter()
			.map(|message| message.item.clone())
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				insert into tags (tag, item)
				select tag, item
				from unnest($1::text[], $2::text[]) as t (tag, item)
				on conflict (tag) do update
				set tag = excluded.tag, item = excluded.item;
			"
		);
		let params = db::params![tags.as_slice(), items.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let statement = indoc!(
			"
				update objects
				set reference_count = reference_count + 1
				from unnest($1::text[]) as t (id)
				where objects.id = t.id;
			"
		);
		let params = db::params![items.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let statement = indoc!(
			"
				update processes
				set reference_count = reference_count + 1
				from unnest($1::text[]) as t (id)
				where processes.id = t.id;
			"
		);
		let params = db::params![items.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let statement = indoc!(
			"
				update cache_entries
				set reference_count = reference_count + 1
				from unnest($1::text[]) as t (i  d)
				where cache_entries.id = t.id;
			"
		);
		let params = db::params![items.as_slice()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	async fn indexer_delete_tags_postgres(
		&self,
		messages: Vec<DeleteTag>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		if messages.is_empty() {
			return Ok(());
		}
		let tags = messages
			.iter()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				delete from tags
				where tag = ANY($1)
				returning item;
			"
		);
		let params = db::params![tags.as_slice()];
		let deleted: Vec<Either<tg::process::Id, tg::object::Id>> = transaction
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if !deleted.is_empty() {
			let statement = indoc!(
				"
					update processes
					set reference_count = reference_count - 1
					from unnest($1::text[]) as t (id)
					where processes.id = t.id;
				"
			);
			let params = db::params![deleted.as_slice()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let statement = indoc!(
				"
					update objects
					set reference_count = reference_count - 1
					from unnest($1::text[]) as t (id)
					where objects.id = t.id;
				"
			);
			let params = db::params![deleted.as_slice()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	pub(super) async fn indexer_handle_queue_postgres(
		&self,
		config: &crate::config::Indexer,
		database: &db::postgres::Database,
	) -> tg::Result<usize> {
		let batch_size = config.queue_batch_size;

		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let mut connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let mut n = batch_size;
		n -= self
			.indexer_handle_complete_object_postgres(&transaction, n)
			.await?;
		n -= self
			.indexer_handle_complete_process_postgres(&transaction, n)
			.await?;
		n -= self
			.indexer_handle_reference_count_cache_entry_postgres(&transaction, n)
			.await?;
		n -= self
			.indexer_handle_reference_count_object_postgres(&transaction, n)
			.await?;
		n -= self
			.indexer_handle_reference_count_process_postgres(&transaction, n)
			.await?;
		let n = batch_size - n;

		if n > 0 {
			self.indexer_increment_transaction_id_postgres(&transaction)
				.await?;
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		}

		Ok(n)
	}

	async fn indexer_handle_complete_object_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				call handle_complete_object($1);
			"
		);
		let params = db::params![n.to_i64().unwrap()];
		let n = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(n)
	}

	async fn indexer_handle_complete_process_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				call handle_complete_process($1);
			"
		);
		let params = db::params![n.to_i64().unwrap()];
		let n = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(n)
	}

	async fn indexer_handle_reference_count_cache_entry_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				call handle_reference_count_cache_entry($1);
			"
		);
		let params = db::params![n.to_i64().unwrap()];
		let n = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(n)
	}

	async fn indexer_handle_reference_count_object_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				call handle_reference_count_object($1);
			"
		);
		let params = db::params![n.to_i64().unwrap()];
		let n = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(n)
	}

	async fn indexer_handle_reference_count_process_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		n: usize,
	) -> tg::Result<usize> {
		let statement = indoc!(
			"
				call handle_reference_count_process($1);
			"
		);
		let params = db::params![n.to_i64().unwrap()];
		let n = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;
		Ok(n)
	}

	async fn indexer_increment_transaction_id_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		let statement = "update transaction_id set id = id + 1;";
		transaction
			.execute(statement.into(), db::params![])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}

	pub(super) async fn indexer_get_transaction_id_postgres(
		&self,
		database: &db::postgres::Database,
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

	pub(super) async fn indexer_get_queue_size_postgres(
		&self,
		database: &db::postgres::Database,
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
