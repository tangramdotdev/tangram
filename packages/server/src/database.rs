#![allow(dead_code)]

use futures::FutureExt as _;
use indoc::formatdoc;
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
			r#"The database has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."#
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
	let sql = formatdoc!(
		r#"
			create table builds (
				id text primary key,
				complete integer not null default 0,
				count integer,
				depth integer not null,
				heartbeat_at text,
				host text not null,
				index_status text,
				index_started_at text,
				log text,
				logs_complete integer not null default 0,
				logs_count integer,
				logs_depth integer,
				logs_weight integer,
				outcome text,
				outcomes_complete integer not null default 0,
				outcomes_count integer,
				outcomes_depth integer,
				outcomes_weight integer,
				retry text not null,
				status text not null,
				target text not null,
				targets_complete integer not null default 0,
				targets_count integer,
				targets_depth integer,
				targets_weight integer,
				touched_at text,
				created_at text not null,
				dequeued_at text,
				started_at text,
				finished_at text
			);

			create index builds_status_created_at_index on builds (status, created_at);

			create index builds_target_created_at_index on builds (target, created_at desc);

			create index builds_index_status_index on builds (index_status, index_started_at);

			create table build_children (
				build text not null,
				position integer not null,
				child text not null
			);

			create unique index build_children_index on build_children (build, position);

			create unique index build_children_build_child_index on build_children (build, child);

			create unique index build_children_build_parent_index on build_children (child, build);

			create index build_children_child_index on build_children (child);

			create table build_logs (
				build text not null,
				position integer not null,
				bytes blob not null
			);

			create unique index build_logs_index on build_logs (build, position);

			create table build_objects (
				build text not null,
				object text not null
			);

			create unique index build_objects_index on build_objects (build, object);

			create index build_objects_object_index on build_objects (object);

			create table objects (
				id text primary key,
				bytes blob,
				children integer not null default 0,
				complete integer not null default 0,
				count integer,
				depth integer,
				index_status text,
				index_started_at text,
				touched_at text,
				weight integer
			);

			create index objects_index_status_index on objects (index_status, index_started_at);

			create table object_children (
				object text not null,
				child text not null
			);

			create unique index object_children_index on object_children (object, child);

			create index object_children_child_index on object_children (child);

			create table tags (
				id integer primary key autoincrement,
				name text not null,
				parent integer not null,
				item text
			);

			create unique index tags_parent_name_index on tags (parent, name);

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
				.execute_batch(&sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
