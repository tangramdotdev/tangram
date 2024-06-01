use super::Server;
use futures::FutureExt as _;
use indoc::formatdoc;
use std::path::Path;
use tangram_client as tg;

impl Server {
	pub(crate) async fn migrate(path: &Path) -> tg::Result<()> {
		let migrations = vec![migration_0000(path).boxed()];

		// Read the version from the version file.
		let version = match tokio::fs::read_to_string(path.join("version")).await {
			Ok(version) => Some(version),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
			Err(error) => {
				return Err(tg::error!(
					source = error,
					"failed to read the path format version"
				))
			},
		};
		let version =
			if let Some(version) = version {
				Some(version.trim().parse::<usize>().map_err(|source| {
					tg::error!(!source, "failed to read the path format version")
				})?)
			} else {
				None
			};

		// If this path is from a newer version of Tangram, then return an error.
		if let Some(version) = version {
			if version >= migrations.len() {
				let path = path.display();
				return Err(tg::error!(
					r#"The path "{path}" has run migrations from a newer version of Tangram. Please run `tg upgrade` to upgrade to the latest version of Tangram."#
				));
			}
		}

		// Run all migrations and update the version file.
		let previously_run_migrations_count = version.map_or(0, |version| version + 1);
		let migrations = migrations
			.into_iter()
			.enumerate()
			.skip(previously_run_migrations_count);
		for (version, migration) in migrations {
			// Run the migration.
			migration.await?;

			// Update the version.
			tokio::fs::write(path.join("version"), version.to_string())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the path format version"))?;
		}

		Ok(())
	}
}

async fn migration_0000(path: &Path) -> tg::Result<()> {
	let path = path.to_owned();
	let connection = rusqlite::Connection::open(path.join("database"))
		.map_err(|source| tg::error!(!source, "failed to create the database"))?;
	connection
		.pragma_update(None, "journal_mode", "wal")
		.map_err(|source| tg::error!(!source, "failed to set the journal mode"))?;
	let sql = formatdoc!(
		r#"
			create table builds (
				id text primary key,
				count integer,
				host text not null,
				log text,
				logs_complete integer not null default 0,
				logs_count integer,
				logs_weight integer,
				outcome text,
				outcomes_complete integer not null default 0,
				outcomes_count integer,
				outcomes_weight integer,
				retry text not null,
				status text not null,
				target text not null,
				targets_complete integer not null default 0,
				targets_count integer,
				targets_weight integer,
				created_at text not null,
				dequeued_at text,
				started_at text,
				finished_at text,
				heartbeat_at text,
				touched_at text
			);

			create index builds_status_created_at_index on builds (status, created_at);

			create index builds_target_created_at_index on builds (target, created_at desc);

			create table build_children (
				build text not null,
				position integer not null,
				child text not null
			);

			create unique index build_children_index on build_children (build, position);

			create unique index build_children_build_child_index on build_children (build, child);

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
				weight integer,
				touched_at text
			);

			create table object_children (
				object text not null,
				child text not null
			);

			create unique index object_children_index on object_children (object, child);

			create index object_children_child_index on object_children (child);

			create table package_versions (
				name text not null,
				version text not null,
				artifact text not null,
				published_at text not null,
				yanked integer not null,
				primary key (name, version)
			);

			create table roots (
				name text primary key,
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
	connection
		.execute_batch(&sql)
		.map_err(|source| tg::error!(!source, "failed to create the database tables"))?;
	Ok(())
}
