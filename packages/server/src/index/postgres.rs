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

		// Prepare cache entry parameters.
		let cache_entry_ids = put_cache_entry_messages
			.iter()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let cache_entry_touched_ats = put_cache_entry_messages
			.iter()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();

		// Prepare put object parameters.
		let put_object_messages: HashMap<&tg::object::Id, &PutObject, fnv::FnvBuildHasher> =
			put_object_messages
				.iter()
				.map(|message| (&message.id, message))
				.collect();
		let object_ids = put_object_messages
			.values()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let object_cache_entries = put_object_messages
			.values()
			.filter_map(|message| message.cache_entry.as_ref())
			.map(|entry| entry.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let object_sizes = put_object_messages
			.values()
			.map(|message| message.size.to_i64().unwrap())
			.collect::<Vec<_>>();
		let object_touched_ats = put_object_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let object_counts = put_object_messages
			.values()
			.map(|message| message.metadata.count.map(|count| count.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let object_depths = put_object_messages
			.values()
			.map(|message| message.metadata.depth.map(|depth| depth.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let object_weights = put_object_messages
			.values()
			.map(|message| {
				message
					.metadata
					.weight
					.map(|weight| weight.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let object_completes = put_object_messages
			.values()
			.map(|message| message.complete)
			.collect::<Vec<_>>();
		let object_children = put_object_messages
			.values()
			.flat_map(|message| {
				message
					.children
					.iter()
					.map(|child| child.to_bytes().to_vec())
			})
			.collect::<Vec<_>>();
		let object_parent_indices = put_object_messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
			})
			.collect::<Vec<_>>();

		// Prepare touch object parameters.
		let touch_object_touched_ats = touch_object_messages
			.iter()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let touch_object_ids = touch_object_messages
			.iter()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();

		// Prepare put process parameters.
		let put_process_messages: HashMap<&tg::process::Id, &PutProcess, fnv::FnvBuildHasher> =
			put_process_messages
				.iter()
				.map(|message| (&message.id, message))
				.collect();
		let process_ids = put_process_messages
			.values()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let process_touched_ats = put_process_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let process_children_completes = put_process_messages
			.values()
			.map(|message| message.complete.children)
			.collect::<Vec<_>>();
		let process_children_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.children
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_command_completes = put_process_messages
			.values()
			.map(|message| message.complete.command)
			.collect::<Vec<_>>();
		let process_command_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.command
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_command_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.command
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_command_weights = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.command
					.weight
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_commands_completes = put_process_messages
			.values()
			.map(|message| message.complete.commands)
			.collect::<Vec<_>>();
		let process_commands_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.commands
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_commands_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.commands
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_commands_weights = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.commands
					.weight
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_output_completes = put_process_messages
			.values()
			.map(|message| message.complete.output)
			.collect::<Vec<_>>();
		let process_output_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.output
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_output_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.output
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_output_weights = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.output
					.weight
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_outputs_completes = put_process_messages
			.values()
			.map(|message| message.complete.outputs)
			.collect::<Vec<_>>();
		let process_outputs_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.outputs
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_outputs_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.outputs
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_outputs_weights = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.outputs
					.weight
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_children = put_process_messages
			.values()
			.flat_map(|message| {
				message
					.children
					.iter()
					.map(|child| child.to_bytes().to_vec())
			})
			.collect::<Vec<_>>();
		let process_child_process_indices = put_process_messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
			})
			.collect::<Vec<_>>();
		let process_child_positions = put_process_messages
			.values()
			.flat_map(|message| (0..message.children.len().to_i64().unwrap()).collect::<Vec<_>>())
			.collect::<Vec<_>>();
		let process_objects = put_process_messages
			.values()
			.flat_map(|message| message.objects.iter().map(|(id, _)| id.to_bytes().to_vec()))
			.collect::<Vec<_>>();
		let process_object_kinds = put_process_messages
			.values()
			.flat_map(|message| message.objects.iter().map(|(_, kind)| kind.to_string()))
			.collect::<Vec<_>>();
		let process_object_process_indices = put_process_messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.objects.len())
			})
			.collect::<Vec<_>>();

		// Prepare touch process parameters.
		let touch_process_touched_ats = touch_process_messages
			.iter()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let touch_process_ids = touch_process_messages
			.iter()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();

		// Prepare put tag parameters.
		let put_tag_tags = put_tag_messages
			.iter()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();
		let put_tag_items = put_tag_messages
			.iter()
			.map(|message| match &message.item {
				Either::Left(process_id) => process_id.to_bytes().to_vec(),
				Either::Right(object_id) => object_id.to_bytes().to_vec(),
			})
			.collect::<Vec<_>>();

		// Prepare delete tag parameters.
		let delete_tags = delete_tag_messages
			.iter()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();

		// Call the procedure.
		let statement = indoc!(
			"
				call handle_messages(
					$1::bytea[],
					$2::int8[],
					$3::bytea[],
					$4::bytea[],
					$5::int8[],
					$6::int8[],
					$7::int8[],
					$8::int8[],
					$9::int8[],
					$10::bool[],
					$11::bytea[],
					$12::int8[],
					$13::int8[],
					$14::bytea[],
					$15::bytea[],
					$16::int8[],
					$17::bool[],
					$18::int8[],
					$19::bool[],
					$20::int8[],
					$21::int8[],
					$22::int8[],
					$23::bool[],
					$24::int8[],
					$25::int8[],
					$26::int8[],
					$27::bool[],
					$28::int8[],
					$29::int8[],
					$30::int8[],
					$31::bool[],
					$32::int8[],
					$33::int8[],
					$34::int8[],
					$35::bytea[],
					$36::int8[],
					$37::int8[],
					$38::bytea[],
					$39::text[],
					$40::int8[],
					$41::int8[],
					$42::bytea[],
					$43::text[],
					$44::bytea[],
					$45::text[]
				);
			"
		);
		transaction
			.inner()
			.execute(
				statement,
				&[
					&cache_entry_ids.as_slice(),
					&cache_entry_touched_ats.as_slice(),
					&object_ids.as_slice(),
					&object_cache_entries.as_slice(),
					&object_sizes.as_slice(),
					&object_touched_ats.as_slice(),
					&object_counts.as_slice(),
					&object_depths.as_slice(),
					&object_weights.as_slice(),
					&object_completes.as_slice(),
					&object_children.as_slice(),
					&object_parent_indices.as_slice(),
					&touch_object_touched_ats.as_slice(),
					&touch_object_ids.as_slice(),
					&process_ids.as_slice(),
					&process_touched_ats.as_slice(),
					&process_children_completes.as_slice(),
					&process_children_counts.as_slice(),
					&process_command_completes.as_slice(),
					&process_command_counts.as_slice(),
					&process_command_depths.as_slice(),
					&process_command_weights.as_slice(),
					&process_commands_completes.as_slice(),
					&process_commands_counts.as_slice(),
					&process_commands_depths.as_slice(),
					&process_commands_weights.as_slice(),
					&process_output_completes.as_slice(),
					&process_output_counts.as_slice(),
					&process_output_depths.as_slice(),
					&process_output_weights.as_slice(),
					&process_outputs_completes.as_slice(),
					&process_outputs_counts.as_slice(),
					&process_outputs_depths.as_slice(),
					&process_outputs_weights.as_slice(),
					&process_children.as_slice(),
					&process_child_process_indices.as_slice(),
					&process_child_positions.as_slice(),
					&process_objects.as_slice(),
					&process_object_kinds.as_slice(),
					&process_object_process_indices.as_slice(),
					&touch_process_touched_ats.as_slice(),
					&touch_process_ids.as_slice(),
					&put_tag_tags.as_slice(),
					&put_tag_items.as_slice(),
					&delete_tags.as_slice(),
				],
			)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to call the handle_messages procedure")
			})?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

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

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let statement = indoc!(
			"
				call handle_queue($1, $2);
			"
		);
		let params = db::params![batch_size.to_i64().unwrap(), 0i64];
		let n = transaction
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(n)
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
					(select count(*) from cache_entry_queue where transaction_id <= $1) +
					(select count(*) from object_queue where transaction_id <= $1) +
					(select count(*) from process_queue where transaction_id <= $1);
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
