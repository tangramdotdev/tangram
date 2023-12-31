use super::Server;
use futures::FutureExt;
use std::path::Path;
use tangram_error::{return_error, Result, Wrap, WrapErr};

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
				return_error!(
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
	let db = rusqlite::Connection::open(path.join("database"))
		.wrap_err("Failed to create the database.")?;
	db.pragma_update(None, "journal_mode", "WAL")
		.wrap_err("Failed to set the journal mode.")?;
	db.execute_batch(
		"
			create table objects (
				id text primary key,
				bytes blob not null
			) strict;

			create table builds (
				id text primary key,
				json text not null
			) strict;

			create index builds_status on builds ((json->'status'));

			create table logs (
				build text not null,
				position int not null,
				bytes blob not null
			) strict;

			create index logs_index on logs (build, position);

			create table assignments (
				target text primary key,
				build text not null
			) strict;

			create table queue (
				json text not null
			) strict;

			create index queue_index on queue ((json->'depth') desc);
		",
	)
	.wrap_err("Failed to create the database tables.")?;

	// Create the artifacts directory.
	let artifacts_path = path.join("artifacts");
	tokio::fs::create_dir_all(&artifacts_path)
		.await
		.wrap_err("Failed to create the artifacts directory.")?;

	// Create the tmp directory.
	let tmp_path = path.join("tmp");
	tokio::fs::create_dir_all(&tmp_path)
		.await
		.wrap_err("Failed to create the tmp directory.")?;

	Ok(())
}
