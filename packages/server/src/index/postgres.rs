use super::message::{
	DeleteTag, PutCacheEntry, PutObject, PutProcess, PutTagMessage, TouchObject, TouchProcess,
};
use crate::Server;
use indoc::indoc;
use num::ToPrimitive as _;
use tangram_either::Either;
use std::collections::HashMap;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

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

		// Handle all messages.
		self.indexer_handle_all_messages_postgres(
			put_cache_entry_messages,
			put_object_messages,
			touch_object_messages,
			put_process_messages,
			touch_process_messages,
			put_tag_messages,
			delete_tag_messages,
			&mut connection,
		)
		.await?;

		Ok(())
	}

	async fn indexer_handle_all_messages_postgres(
		&self,
		put_cache_entry_messages: Vec<PutCacheEntry>,
		put_object_messages: Vec<PutObject>,
		touch_object_messages: Vec<TouchObject>,
		put_process_messages: Vec<PutProcess>,
		touch_process_messages: Vec<TouchProcess>,
		put_tag_messages: Vec<PutTagMessage>,
		delete_tag_messages: Vec<DeleteTag>,
		connection: &mut db::postgres::Connection,
	) -> tg::Result<()> {
		// Prepare cache entry parameters.
		let cache_entry_ids = put_cache_entry_messages
			.iter()
			.map(|message| message.id.to_bytes())
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
			.map(|message| message.id.to_bytes())
			.collect::<Vec<_>>();
		let object_cache_entries = put_object_messages
			.values()
			.filter_map(|message| message.cache_entry.as_ref())
			.map(|entry| entry.to_bytes())
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
			.flat_map(|message| message.children.iter().map(
				|child| child.to_bytes()
			))
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
			.map(|message| message.id.to_bytes())
			.collect::<Vec<_>>();

		// Prepare put process parameters.
		let put_process_messages: HashMap<&tg::process::Id, &PutProcess, fnv::FnvBuildHasher> =
			put_process_messages
				.iter()
				.map(|message| (&message.id, message))
				.collect();
		let process_ids = put_process_messages
			.values()
			.map(|message| message.id.to_bytes())
			.collect::<Vec<_>>();
		let process_touched_ats = put_process_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let process_children_complete = put_process_messages
			.values()
			.map(|message| message.complete.children)
			.collect::<Vec<_>>();
		let process_children_counts = put_process_messages
			.values()
			.map(|message| message.metadata.count.map(|value| value.to_i64().unwrap()))
			.collect::<Vec<_>>();
		let process_commands_complete = put_process_messages
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
		let process_outputs_complete = put_process_messages
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
			.flat_map(|message| message.children.iter().map(|child| child.to_bytes()))
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
			.flat_map(|message| message.objects.iter().map(|(id, _)| id.to_bytes()))
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
			.map(|message| message.id.to_bytes())
			.collect::<Vec<_>>();

		// Prepare put tag parameters.
		let put_tag_tags = put_tag_messages
			.iter()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();
		let put_tag_items = put_tag_messages
			.iter()
			.map(|message| match &message.item {
				Either::Left(process_id) => process_id.to_bytes(),
				Either::Right(object_id) => object_id.to_bytes()
			})
			.collect::<Vec<_>>();

		// Prepare delete tag parameters.
		let delete_tag_names = delete_tag_messages
			.iter()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();

		// Call the proceudre.
		let statement = indoc!(
			"
				call handle_indexer_messages(
					$1::text[], $2::int8[],
					$3::text[], $4::text[], $5::int8[], $6::int8[], $7::int8[], $8::int8[], $9::int8[], $10::bool[], $11::text[], $12::int8[],
					$13::int8[], $14::text[],
					$15::text[], $16::int8[], $17::bool[], $18::int8[], $19::bool[], $20::int8[], $21::int8[], $22::int8[], $23::bool[], $24::int8[], $25::int8[], $26::int8[], $27::text[], $28::int8[], $29::int8[], $30::text[], $31::text[], $32::int8[],
					$33::int8[], $34::text[],
					$35::text[], $36::text[],
					$37::text[]
				);
			"
		);

		connection
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
					&process_children_complete.as_slice(),
					&process_children_counts.as_slice(),
					&process_commands_complete.as_slice(),
					&process_commands_counts.as_slice(),
					&process_commands_depths.as_slice(),
					&process_commands_weights.as_slice(),
					&process_outputs_complete.as_slice(),
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
					&delete_tag_names.as_slice(),
				],
			)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to call the handle_indexer_messages procedure"
				)
			})?;

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
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Call the single procedure that manages its own transaction
		let statement = indoc!(
			"
				call handle_indexer_queue($1, $2);
			"
		);
		let params = db::params![batch_size.to_i64().unwrap(), 0i64];

		let n = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to call the procedure"))?;

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
