use super::Server;
use futures::FutureExt;
use std::path::Path;
use tangram_error::{error, Result, Wrap, WrapErr};

impl Server {
	pub async fn migrate(path: &Path) -> Result<()> {
		let migrations = vec![migration_0000(path).boxed()];

		// Read the version from the version file.
		let version = match tokio::fs::read_to_string(path.join("version")).await {
			Ok(version) => Some(version),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
			Err(error) => return Err(error.wrap("Failed to read the path format version.")),
		};
		let version = if let Some(version) = version {
			Some(
				version
					.trim()
					.parse::<usize>()
					.wrap_err("Failed to read the path format version.")?,
			)
		} else {
			None
		};

		// If this path is from a newer version of Tangram, then return an error.
		if let Some(version) = version {
			if version >= migrations.len() {
				let path = path.display();
				error!(
					r#"The path "{path}" has run migrations from a newer version of Tangram. Please run `tg upgrade` to upgrade to the latest version of Tangram."#
				);
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
				.wrap_err("Failed to write the path format version.")?;
		}

		Ok(())
	}
}

async fn migration_0000(path: &Path) -> Result<()> {
	let path = path.to_owned();

	// Create the database.
	let connection = rusqlite::Connection::open(path.join("database"))
		.wrap_err("Failed to create the database.")?;
	connection
		.pragma_update(None, "journal_mode", "wal")
		.wrap_err("Failed to set the journal mode.")?;
	let sql = "
		create table builds (
			id text primary key,
			children text,
			descendants integer,
			host text not null,
			log text,
			outcome text,
			retry text not null,
			status text not null,
			target text not null,
			weight integer,
			created_at text not null,
			queued_at text,
			started_at text,
			finished_at text
		) strict;

		create index builds_status_created_at_index on builds (status, created_at);

		create index builds_target_created_at_index on builds (target, created_at desc);

		create table build_children (
			build text,
			position integer,
			child text
		) strict;

		create unique index build_children_index on build_children (build, child);

		create index build_children_child_index on build_children (child);

		create table build_logs (
			build text not null,
			position integer not null,
			bytes blob not null
		) strict;

		create unique index build_logs_index on build_logs (build, position);

		create table build_objects (
			build text not null,
			object text not null
		) strict;

		create unique index build_objects_index on build_objects (build, object);

		create index build_objects_child_index on build_objects (object);

		create table objects (
			id text primary key,
			bytes blob not null,
			complete integer not null,
			weight integer
		) strict;

		create table object_children (
			object text not null,
			child text not null
		) strict;

		create unique index object_children_index on object_children (object, child);

		create index object_children_child_index on object_children (child);

		create table packages (
			name text primary key
		) strict;

		create table package_versions (
			name text not null references packages (name),
			version text not null,
			id text not null,
			primary key (name, version)
		) strict;

		create table users (
			id text primary key,
			email text not null
		) strict;

		create table tokens (
			id text primary key,
			user_id text not null references users (id)
		) strict;

		create table logins (
			id text primary key,
			url text not null,
			token text references tokens (id)
		) strict;
	";
	connection
		.execute_batch(sql)
		.wrap_err("Failed to create the database tables.")?;

	// Create the checkouts directory.
	let checkouts_path = path.join("checkouts");
	tokio::fs::create_dir_all(&checkouts_path)
		.await
		.wrap_err("Failed to create the checkouts directory.")?;

	// Create the tmp directory.
	let tmp_path = path.join("tmp");
	tokio::fs::create_dir_all(&tmp_path)
		.await
		.wrap_err("Failed to create the tmp directory.")?;

	Ok(())
}
