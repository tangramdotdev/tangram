use super::{
	DeleteTagMessage, ProcessObjectKind, PutCacheEntryMessage, PutObjectMessage, PutProcessMessage,
	PutTagMessage, TouchObjectMessage, TouchProcessMessage,
};
use crate::Server;
use indoc::indoc;
use num::ToPrimitive as _;
use std::collections::HashMap;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn indexer_task_handle_messages_postgres(
		&self,
		database: &db::postgres::Database,
		put_cache_entry_messages: Vec<PutCacheEntryMessage>,
		put_object_messages: Vec<PutObjectMessage>,
		touch_object_messages: Vec<TouchObjectMessage>,
		put_process_messages: Vec<PutProcessMessage>,
		touch_process_messages: Vec<TouchProcessMessage>,
		put_tag_messages: Vec<PutTagMessage>,
		delete_tag_messages: Vec<DeleteTagMessage>,
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

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	async fn indexer_put_cache_entries_postgres(
		&self,
		messages: Vec<PutCacheEntryMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					insert or replace into cache_entries (id, touched_at)
					values ($1, $2);
				"
			);
			let params = db::params![message.id.to_string(), message.touched_at];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_put_objects_postgres(
		&self,
		messages: Vec<PutObjectMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		// Get the unique messages.
		let unique_messages: HashMap<&tg::object::Id, &PutObjectMessage, fnv::FnvBuildHasher> =
			messages
				.iter()
				.map(|message| (&message.id, message))
				.collect();

		// Insert into the objects and object_children tables.
		let ids = unique_messages
			.values()
			.map(|message| message.id.to_string())
			.collect::<Vec<_>>();
		let size = unique_messages
			.values()
			.map(|message| message.size.to_i64().unwrap())
			.collect::<Vec<_>>();
		let touched_ats = unique_messages
			.values()
			.map(|message| message.touched_at)
			.collect::<Vec<_>>();
		let children = unique_messages
			.values()
			.flat_map(|message| message.children.iter().map(ToString::to_string))
			.collect::<Vec<_>>();
		let parent_indices = unique_messages
			.values()
			.enumerate()
			.flat_map(|(index, message)| {
				std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
			})
			.collect::<Vec<_>>();
		let cache_references = unique_messages
			.values()
			.filter_map(|message| message.cache_reference.as_ref())
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		let statement = indoc!(
			"
				call insert_objects_and_children(
					$1::text[],
					$2::text[],
					$3::int8[],
					$4::int8[],
					$5::text[],
					$6::int8[]
				);
			"
		);
		transaction
			.inner()
			.execute(
				statement,
				&[
					&ids.as_slice(),
					&cache_references.as_slice(),
					&size.as_slice(),
					&touched_ats.as_slice(),
					&children.as_slice(),
					&parent_indices.as_slice(),
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the procedure"))?;

		Ok(())
	}

	async fn indexer_touch_objects_postgres(
		&self,
		messages: Vec<TouchObjectMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					update objects
					set touched_at = $1
					where id = $2;
				"
			);
			let params = db::params![message.touched_at, message.id.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_put_processes_postgres(
		&self,
		messages: Vec<PutProcessMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			// Insert the objects.
			let objects: Vec<&tg::object::Id> = message.objects.iter().map(|(id, _)| id).collect();
			let kinds: Vec<&ProcessObjectKind> =
				message.objects.iter().map(|(_, kind)| kind).collect();
			if !message.objects.is_empty() {
				let statement = indoc!(
					"
						insert into process_objects (process, object, kind)
						select $1, unnest($2::text[]), unnest($3::text[])
						on conflict (process, object, kind) do nothing;
					"
				);
				transaction
					.inner()
					.execute(
						statement,
						&[
							&message.id.to_string(),
							&objects.iter().map(ToString::to_string).collect::<Vec<_>>(),
							&kinds.iter().map(ToString::to_string).collect::<Vec<_>>(),
						],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the children.
			if let Some(children) = &message.children {
				let positions: Vec<i64> = (0..children.len().to_i64().unwrap()).collect();
				let statement = indoc!(
					"
						insert into process_children (process, position, child, path, tag)
						select $1, unnest($2::int8[]), unnest($3::text[]), unnest($4::text[]), unnest($5::text[])
						on conflict (process, child) do nothing;
					"
				);
				transaction
					.inner()
					.execute(
						statement,
						&[
							&message.id.to_string(),
							&positions.as_slice(),
							&children
								.iter()
								.map(|referent| referent.item.to_string())
								.collect::<Vec<_>>(),
							&children
								.iter()
								.map(|referent| {
									referent.path().map(|path| path.display().to_string())
								})
								.collect::<Vec<_>>(),
							&children
								.iter()
								.map(|referent| referent.tag().map(ToString::to_string))
								.collect::<Vec<_>>(),
						],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Insert the process.
			let process_statement = indoc!(
				"
					insert into processes (id, touched_at)
					values ($1, $2)
					on conflict (id) do update set
						touched_at = $2;
				"
			);
			transaction
				.inner()
				.execute(
					process_statement,
					&[&message.id.to_string(), &message.touched_at],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_touch_processes_postgres(
		&self,
		messages: Vec<TouchProcessMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					update processes
					set touched_at = $1
					where id = $2;
				"
			);
			let params = db::params![message.touched_at, message.id.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_put_tags_postgres(
		&self,
		messages: Vec<PutTagMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					insert into tags (tag, item)
					values ($1, $2)
					on conflict (tag) do update
					set tag = $1, item = $2;
				"
			);
			let params = db::params![message.tag, message.item];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}

	async fn indexer_delete_tags_postgres(
		&self,
		messages: Vec<DeleteTagMessage>,
		transaction: &db::postgres::Transaction<'_>,
	) -> tg::Result<()> {
		for message in messages {
			let statement = indoc!(
				"
					delete from tags
					where tag = $1;
				"
			);
			let params = db::params![message.tag];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}
}
