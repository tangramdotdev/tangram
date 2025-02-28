#![allow(dead_code)]

use futures::FutureExt as _;
use indoc::indoc;
use tangram_client as tg;
use tangram_database::{self as db, Database as _};
use tangram_either::Either;

pub type Error = db::either::Error<db::sqlite::Error, db::postgres::Error>;

pub type Database = Either<db::sqlite::Database, db::postgres::Database>;

#[allow(clippy::module_name_repetitions)]
pub type DatabaseOptions = Either<db::sqlite::DatabaseOptions, db::postgres::DatabaseOptions>;

pub type Connection = Either<db::sqlite::Connection, db::postgres::Connection>;

pub type ConnectionOptions = Either<db::sqlite::ConnectionOptions, db::postgres::ConnectionOptions>;

pub type Transaction<'a> = Either<db::sqlite::Transaction<'a>, db::postgres::Transaction<'a>>;

pub async fn migrate(database: &Database) -> tg::Result<()> {
	if database.is_right() {
		return Ok(());
	}

	let migrations = vec![migration_0000(database).boxed()];

	let version = match database {
		Either::Left(database) => {
			let connection = database
				.connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			connection
				.with(|connection| {
					connection
						.pragma_query_value(None, "user_version", |row| {
							Ok(row.get_unwrap::<_, usize>(0))
						})
						.map_err(|source| tg::error!(!source, "failed to get the version"))
				})
				.await?
		},
		Either::Right(_) => {
			unreachable!()
		},
	};

	// If this path is from a newer version of Tangram, then return an error.
	if version > migrations.len() {
		return Err(tg::error!(
			r"The database has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	// Run all migrations and update the version file.
	let migrations = migrations.into_iter().enumerate().skip(version);
	for (version, migration) in migrations {
		// Run the migration.
		migration.await?;

		// Update the version.
		match database {
			Either::Left(database) => {
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
			},
			Either::Right(_) => {
				unreachable!()
			},
		}
	}

	Ok(())
}

async fn migration_0000(database: &Database) -> tg::Result<()> {
	let sql = indoc!(
		r#"
			create table blobs (
				id text primary key,
				reference_count integer
			);

			create index blobs_reference_count_zero_index on blobs ((1)) where reference_count = 0;

			create table blob_references (
				id text primary key,
				blob text not null,
				position integer not null,
				length integer not null
			);

			create trigger blobs_increment_reference_count_trigger
			after insert on blob_references
			for each row
			begin
				update blobs set reference_count = reference_count + 1
				where id = new.blob;
			end;

			create trigger blobs_decrement_reference_count_trigger
			after delete on blob_references
			for each row
			begin
				update blobs set reference_count = reference_count - 1
				where id = old.blob;
			end;

			create table objects (
				id text primary key,
				complete integer not null default 0,
				count integer,
				depth integer,
				incomplete_children integer,
				reference_count integer,
				size integer not null,
				touched_at text,
				weight integer
			);

			create index objects_complete_incomplete_children_index on objects ((1)) where complete = 0 and incomplete_children = 0;

			create index objects_reference_count_zero_index on objects (touched_at) where reference_count = 0;

			create trigger objects_set_reference_count_trigger
			after insert on objects
			for each row
			begin
				update objects
				set reference_count = (
					(select count(*) from object_children where child = new.id) +
					(select count(*) from process_objects where object = new.id) +
					(select count(*) from tags where item = new.id)
				)
				where id = new.id;
			end;

			create trigger objects_set_incomplete_children_trigger
			after insert on objects
			for each row
			when (new.incomplete_children is null)
			begin
				update objects
				set incomplete_children = (
					select count(*)
					from object_children
					left join objects child_objects on child_objects.id = object_children.child
					where object_children.object = new.id and (child_objects.complete is null or child_objects.complete = 0)
				)
				where id = new.id;
			end;

			create trigger objects_update_incomplete_children_trigger
			after update of complete on objects
			for each row
			when (old.complete = 0 and new.complete = 1)
			begin
				update objects
				set incomplete_children = incomplete_children - 1
				where id in (
					select object
					from object_children
					where child = new.id
				);
			end;

			create table object_children (
				object text not null,
				child text not null
			);

			create unique index object_children_index on object_children (object, child);

			create index object_children_child_index on object_children (child);

			create trigger object_children_increment_reference_count_trigger
			after insert on object_children
			for each row
			begin
				update objects
				set reference_count = objects.reference_count + 1
				where id = new.child;
			end;

			create trigger object_children_decrement_reference_count_trigger
			after delete on object_children
			for each row
			begin
				update objects
				set reference_count = objects.reference_count - 1
				where id = old.child;
			end;

			create table processes (
				cacheable integer not null,
				checksum text,
				command text not null,
				commands_complete integer not null default 0,
				commands_count integer,
				commands_depth integer,
				commands_weight integer,
				complete integer not null default 0,
				count integer,
				created_at text not null,
				cwd text,
				dequeued_at text,
				enqueued_at text,
				env text,
				error text,
				exit text,
				finished_at text,
				heartbeat_at text,
				host text not null,
				id text primary key,
				log text,
				logs_complete integer not null default 0,
				logs_count integer,
				logs_depth integer,
				logs_weight integer,
				network integer not null,
				output text,
				outputs_complete integer not null default 0,
				outputs_count integer,
				outputs_depth integer,
				outputs_weight integer,
				reference_count integer,
				retry integer not null,
				started_at text,
				status text not null,
				touched_at text
			);

			create index processes_command_index on processes (command);

			create index processes_status_index on processes (status);

			create index processes_reference_count_zero_index on processes (touched_at) where reference_count = 0;

			create trigger processes_set_reference_count_trigger
			after insert on processes
			for each row
			begin
				update processes
				set reference_count = (
					(select count(*) from process_children where child = new.id) +
					(select count(*) from tags where item = new.id)
				)
				where id = new.id;
			end;

			create table process_children (
				process text not null,
				child text not null,
				position integer not null
			);

			create unique index process_children_process_child_index on process_children (process, child);

			create index process_children_index on process_children (process, position);

			create index process_children_child_process_index on process_children (child, process);

			create trigger process_children_increment_reference_count_trigger
			after insert on process_children
			for each row
			begin
				update processes
				set reference_count = processes.reference_count + 1
				where id = new.child;
			end;

			create trigger process_children_decrement_reference_count_trigger
			after delete on process_children
			for each row
			begin
				update processes
				set reference_count = processes.reference_count - 1
				where id = old.child;
			end;

			create table process_logs (
				process text not null,
				bytes blob not null,
				position integer not null
			);

			create index process_logs_process_position_index on process_logs (process, position);

			create table process_objects (
				process text not null,
				object text not null
			);

			create unique index process_objects_index on process_objects (process, object);

			create index process_objects_object_index on process_objects (object);

			create trigger process_objects_increment_reference_count_trigger
			after insert on process_objects
			begin
				update objects
				set reference_count = reference_count + 1
				where id = new.object;
			end;

			create trigger process_objects_decrement_reference_count_trigger
			after delete on process_objects
			begin
				update objects
				set reference_count = reference_count - 1
				where id = old.object;
			end;

			create table remotes (
				name text primary key,
				url text not null
			);

			create table tags (
				tag text primary key,
				item text not null
			);

			create trigger tags_increment_reference_count_trigger
			after insert on tags
			for each row
			begin
				update objects set reference_count = reference_count + 1
				where id = new.item;

				update processes set reference_count = reference_count + 1
				where id = new.item;
			end;

			create trigger tags_decrement_reference_count_trigger
			after delete on tags
			for each row
			begin
				update objects set reference_count = reference_count - 1
				where id = old.item;

				update processes set reference_count = reference_count - 1
				where id = old.item;
			end;

			create table users (
				id text primary key,
				email text not null
			);

			create table tokens (
				id text primary key,
				"user" text not null
			);
		"#
	);
	let database = database.as_ref().unwrap_left();
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
	connection
		.with(move |connection| {
			let sql =
				"insert into remotes (name, url) values ('default', 'https://cloud.tangram.dev');";
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
