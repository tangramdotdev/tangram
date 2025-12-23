use {
	super::message::{
		DeleteTag, PutCacheEntry, PutObject, PutProcess, PutTagMessage, TouchObject, TouchProcess,
	},
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	std::collections::{HashMap, HashSet},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	#[expect(clippy::too_many_arguments)]
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

		let put_cache_entry_messages: HashMap<
			&tg::artifact::Id,
			&PutCacheEntry,
			tg::id::BuildHasher,
		> = put_cache_entry_messages.iter().fold(
			HashMap::with_hasher(tg::id::BuildHasher),
			|mut messages, message| {
				messages
					.entry(&message.id)
					.and_modify(|existing| {
						if message.touched_at > existing.touched_at {
							*existing = message;
						}
					})
					.or_insert(message);
				messages
			},
		);
		let cache_entry_ids = put_cache_entry_messages
			.values()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let cache_entry_touched_ats = put_cache_entry_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();

		// Prepare put object parameters.
		let put_object_messages: HashMap<&tg::object::Id, &PutObject, tg::id::BuildHasher> =
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
		let object_node_sizes = put_object_messages
			.values()
			.map(|message| message.metadata.node.size.to_i64().unwrap())
			.collect::<Vec<_>>();
		let object_node_solvables = put_object_messages
			.values()
			.map(|message| message.metadata.node.solvable)
			.collect::<Vec<_>>();
		let object_node_solveds = put_object_messages
			.values()
			.map(|message| message.metadata.node.solved)
			.collect::<Vec<_>>();
		let object_touched_ats = put_object_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let object_subtree_counts = put_object_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.count
					.map(|count| count.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let object_subtree_depths = put_object_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.depth
					.map(|depth| depth.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let object_subtree_sizes = put_object_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.size
					.map(|size| size.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let object_subtree_solvables = put_object_messages
			.values()
			.map(|message| message.metadata.subtree.solvable)
			.collect::<Vec<_>>();
		let object_subtree_solveds = put_object_messages
			.values()
			.map(|message| message.metadata.subtree.solved)
			.collect::<Vec<_>>();
		let object_subtree_storeds = put_object_messages
			.values()
			.map(|message| message.stored.subtree)
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
		let touch_object_messages: HashMap<&tg::object::Id, &TouchObject, tg::id::BuildHasher> =
			touch_object_messages.iter().fold(
				HashMap::with_hasher(tg::id::BuildHasher),
				|mut messages, message| {
					messages
						.entry(&message.id)
						.and_modify(|existing| {
							if message.touched_at > existing.touched_at {
								*existing = message;
							}
						})
						.or_insert(message);
					messages
				},
			);
		let touch_object_touched_ats = touch_object_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let touch_object_ids = touch_object_messages
			.values()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();

		// Prepare put process parameters.
		let put_process_messages: HashMap<&tg::process::Id, &PutProcess, tg::id::BuildHasher> =
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
		let process_node_command_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.command
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_command_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.command
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_command_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.command
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_command_storeds = put_process_messages
			.values()
			.map(|message| message.stored.node_command)
			.collect::<Vec<_>>();
		let process_node_error_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.error
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_error_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.error
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_error_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.error
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_error_storeds = put_process_messages
			.values()
			.map(|message| message.stored.node_error)
			.collect::<Vec<_>>();
		let process_node_log_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.log
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_log_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.log
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_log_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.log
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_log_storeds = put_process_messages
			.values()
			.map(|message| message.stored.node_log)
			.collect::<Vec<_>>();
		let process_node_output_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.output
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_output_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.output
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_output_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.node
					.output
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_node_output_storeds = put_process_messages
			.values()
			.map(|message| message.stored.node_output)
			.collect::<Vec<_>>();
		let process_subtree_command_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.command
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_command_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.command
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_command_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.command
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_command_storeds = put_process_messages
			.values()
			.map(|message| message.stored.subtree_command)
			.collect::<Vec<_>>();
		let process_subtree_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_error_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.error
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_error_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.error
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_error_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.error
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_error_storeds = put_process_messages
			.values()
			.map(|message| message.stored.subtree_error)
			.collect::<Vec<_>>();
		let process_subtree_log_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.log
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_log_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.log
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_log_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.log
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_log_storeds = put_process_messages
			.values()
			.map(|message| message.stored.subtree_log)
			.collect::<Vec<_>>();
		let process_subtree_output_counts = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.output
					.count
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_output_depths = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.output
					.depth
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_output_sizes = put_process_messages
			.values()
			.map(|message| {
				message
					.metadata
					.subtree
					.output
					.size
					.map(|value| value.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_subtree_output_storeds = put_process_messages
			.values()
			.map(|message| message.stored.subtree_output)
			.collect::<Vec<_>>();
		let process_subtree_storeds = put_process_messages
			.values()
			.map(|message| message.stored.subtree)
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
			.flat_map(|message| {
				message
					.objects
					.iter()
					.map(|(_, kind)| kind.to_i64().unwrap())
			})
			.collect::<Vec<_>>();
		let process_object_process_indices = put_process_messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.objects.len())
			})
			.collect::<Vec<_>>();

		// Prepare touch process parameters.
		let touch_process_messages: HashMap<&tg::process::Id, &TouchProcess, tg::id::BuildHasher> =
			touch_process_messages.iter().fold(
				HashMap::with_hasher(tg::id::BuildHasher),
				|mut messages, message| {
					messages
						.entry(&message.id)
						.and_modify(|existing| {
							if message.touched_at > existing.touched_at {
								*existing = message;
							}
						})
						.or_insert(message);
					messages
				},
			);
		let touch_process_touched_ats = touch_process_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let touch_process_ids = touch_process_messages
			.values()
			.map(|message| message.id.to_bytes().to_vec())
			.collect::<Vec<_>>();

		// Prepare put tag parameters.
		let put_tag_messages: HashMap<&str, &PutTagMessage, fnv::FnvBuildHasher> = put_tag_messages
			.iter()
			.map(|message| (message.tag.as_str(), message))
			.collect();
		let put_tag_tags = put_tag_messages
			.values()
			.map(|message| message.tag.clone())
			.collect::<Vec<_>>();
		let put_tag_items = put_tag_messages
			.values()
			.map(|message| match &message.item {
				tg::Either::Left(process_id) => process_id.to_bytes().to_vec(),
				tg::Either::Right(object_id) => object_id.to_bytes().to_vec(),
			})
			.collect::<Vec<_>>();

		// Prepare delete tag parameters.
		let delete_tags: HashSet<_, fnv::FnvBuildHasher> = delete_tag_messages
			.iter()
			.map(|message| message.tag.as_str())
			.collect();
		let delete_tags = delete_tags
			.into_iter()
			.map(ToOwned::to_owned)
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
					$6::bool[],
					$7::bool[],
					$8::int8[],
					$9::int8[],
					$10::int8[],
					$11::int8[],
					$12::bool[],
					$13::bool[],
					$14::bool[],
					$15::bytea[],
					$16::int8[],
					$17::int8[],
					$18::bytea[],
					$19::bytea[],
					$20::int8[],
					$21::int8[],
					$22::int8[],
					$23::int8[],
					$24::bool[],
					$25::int8[],
					$26::int8[],
					$27::int8[],
					$28::bool[],
					$29::int8[],
					$30::int8[],
					$31::int8[],
					$32::bool[],
					$33::int8[],
					$34::int8[],
					$35::int8[],
					$36::bool[],
					$37::int8[],
					$38::int8[],
					$39::int8[],
					$40::bool[],
					$41::int8[],
					$42::int8[],
					$43::int8[],
					$44::int8[],
					$45::bool[],
					$46::int8[],
					$47::int8[],
					$48::int8[],
					$49::bool[],
					$50::int8[],
					$51::int8[],
					$52::int8[],
					$53::bool[],
					$54::bool[],
					$55::bytea[],
					$56::int8[],
					$57::int8[],
					$58::bytea[],
					$59::int8[],
					$60::int8[],
					$61::int8[],
					$62::bytea[],
					$63::text[],
					$64::bytea[],
					$65::text[]
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
					&object_node_sizes.as_slice(),
					&object_node_solvables.as_slice(),
					&object_node_solveds.as_slice(),
					&object_touched_ats.as_slice(),
					&object_subtree_counts.as_slice(),
					&object_subtree_depths.as_slice(),
					&object_subtree_sizes.as_slice(),
					&object_subtree_solvables.as_slice(),
					&object_subtree_solveds.as_slice(),
					&object_subtree_storeds.as_slice(),
					&object_children.as_slice(),
					&object_parent_indices.as_slice(),
					&touch_object_touched_ats.as_slice(),
					&touch_object_ids.as_slice(),
					&process_ids.as_slice(),
					&process_touched_ats.as_slice(),
					&process_node_command_counts.as_slice(),
					&process_node_command_depths.as_slice(),
					&process_node_command_sizes.as_slice(),
					&process_node_command_storeds.as_slice(),
					&process_node_error_counts.as_slice(),
					&process_node_error_depths.as_slice(),
					&process_node_error_sizes.as_slice(),
					&process_node_error_storeds.as_slice(),
					&process_node_log_counts.as_slice(),
					&process_node_log_depths.as_slice(),
					&process_node_log_sizes.as_slice(),
					&process_node_log_storeds.as_slice(),
					&process_node_output_counts.as_slice(),
					&process_node_output_depths.as_slice(),
					&process_node_output_sizes.as_slice(),
					&process_node_output_storeds.as_slice(),
					&process_subtree_command_counts.as_slice(),
					&process_subtree_command_depths.as_slice(),
					&process_subtree_command_sizes.as_slice(),
					&process_subtree_command_storeds.as_slice(),
					&process_subtree_counts.as_slice(),
					&process_subtree_error_counts.as_slice(),
					&process_subtree_error_depths.as_slice(),
					&process_subtree_error_sizes.as_slice(),
					&process_subtree_error_storeds.as_slice(),
					&process_subtree_log_counts.as_slice(),
					&process_subtree_log_depths.as_slice(),
					&process_subtree_log_sizes.as_slice(),
					&process_subtree_log_storeds.as_slice(),
					&process_subtree_output_counts.as_slice(),
					&process_subtree_output_depths.as_slice(),
					&process_subtree_output_sizes.as_slice(),
					&process_subtree_output_storeds.as_slice(),
					&process_subtree_storeds.as_slice(),
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
