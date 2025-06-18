#![allow(dead_code)]

use futures::FutureExt as _;
use indoc::indoc;
use num::ToPrimitive as _;
use rusqlite as sqlite;
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

pub fn initialize(connection: &sqlite::Connection) -> sqlite::Result<()> {
	connection.pragma_update(None, "auto_vaccum", "incremental")?;
	connection.pragma_update(None, "busy_timeout", "5000")?;
	connection.pragma_update(None, "cache_size", "-20000")?;
	connection.pragma_update(None, "foreign_keys", "on")?;
	connection.pragma_update(None, "journal_mode", "wal")?;
	connection.pragma_update(None, "mmap_size", "2147483648")?;
	connection.pragma_update(None, "recursive_triggers", "on")?;
	connection.pragma_update(None, "synchronous", "normal")?;
	connection.pragma_update(None, "temp_store", "memory")?;
	let function = |context: &sqlite::functions::Context| -> sqlite::Result<sqlite::types::Value> {
		let string = context.get::<String>(0)?;
		let delim = context.get::<String>(1)?;
		let index = context.get::<i64>(2)? - 1;
		if index < 0 {
			return Ok(sqlite::types::Value::Null);
		}
		let string = string
			.split(&delim)
			.nth(index.to_usize().unwrap())
			.map(ToOwned::to_owned)
			.map_or(sqlite::types::Value::Null, sqlite::types::Value::Text);
		Ok(string)
	};
	let flags = sqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC
		| sqlite::functions::FunctionFlags::SQLITE_UTF8;
	connection.create_scalar_function("split_part", 3, flags, function)?;
	Ok(())
}

async fn migration_0000(database: &Database) -> tg::Result<()> {
	let sql = indoc!(
		r#"
			create table processes (
				actual_checksum text,
				cacheable integer not null,
				command text not null,
				created_at integer not null,
				dequeued_at integer,
				depth integer,
				enqueued_at integer,
				error text,
				exit integer,
				expected_checksum text,
				finished_at integer,
				heartbeat_at integer,
				host text not null,
				id text primary key,
				log text,
				mounts text,
				network integer not null,
				output text,
				retry integer not null,
				started_at integer,
				status text not null,
				stderr text,
				stdin text,
				stdout text,
				touched_at integer
			);

			create index processes_command_index on processes (command);

			create index processes_depth_index on processes (depth) where status = 'started';

			create index processes_heartbeat_at_index on processes (heartbeat_at) where status = 'started';

			create index processes_status_index on processes (status);

			create trigger processes_delete_trigger
			after delete on processes
			for each row
			begin
				delete from process_children
				where process = old.id;
			end;

			create trigger processes_update_depth_trigger
			after update of depth on processes
			for each row
			begin
				update processes
				set depth = updates.depth
				from (
					select 
						processes.id,
						coalesce(max(child_processes.depth), 0) + 1 as depth
					from processes
					left join process_children on process_children.process = processes.id
					left join processes child_processes on child_processes.id = process_children.child
					group by processes.id
				) as updates 
				where processes.id = updates.id and processes.id in (
					select process
					from process_children
					where process_children.child = new.id
				);
			end;

			create table process_children (
				process text not null,
				child text not null,
				position integer not null,
				path text,
				tag text
			);

			create trigger process_children_insert_depth_trigger
			after insert on process_children
			for each row
			begin
				update processes
				set depth = updates.depth
				from (
					select 
						processes.id,
						coalesce(max(child_processes.depth), 0) + 1 as depth
					from processes
					left join process_children on process_children.process = processes.id
					left join processes child_processes on child_processes.id = process_children.child
					group by processes.id
				) as updates 
				where processes.id = updates.id and processes.id in (
					select process
					from process_children
					where process_children.child = new.child
				);
			end;

			create unique index process_children_process_child_index on process_children (process, child);

			create unique index process_children_index on process_children (process, position);

			create index process_children_child_process_index on process_children (child);

			create table pipes (
				id text primary key,
				created_at integer not null
			);

			create table ptys (
				id text primary key,
				created_at integer not null,
				size text
			);

			create table remotes (
				name text primary key,
				url text not null
			);

			create table tags (
				tag text primary key,
				item text not null
			);

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
