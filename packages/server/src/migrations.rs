use {futures::FutureExt as _, tangram_client::prelude::*};

#[path = "migrations/0000.rs"]
mod migration_0000;

impl crate::Server {
	pub(super) async fn migrate(&self) -> tg::Result<()> {
		// Read the directory version.
		let version_path = self.path.join("version");
		let version = match tokio::fs::read_to_string(&version_path).await {
			Ok(string) => Some(
				string
					.parse::<usize>()
					.map_err(|error| tg::error!(!error, "invalid version file"))?,
			),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the version file"));
			},
		};

		// Validate the version.
		let migrations = [self.migrate_0000().boxed()];
		if version.is_some_and(|version| version >= migrations.len()) {
			return Err(tg::error!(
				"the data directory was created with a newer version of tangram"
			));
		}

		// Run the pending migrations and record each completed version.
		for (index, migration) in migrations.into_iter().enumerate() {
			if version.is_some_and(|version| version >= index) {
				continue;
			}
			migration.await?;
			tokio::fs::write(&version_path, index.to_string())
				.await
				.map_err(|error| tg::error!(!error, "failed to write the version file"))?;
		}

		Ok(())
	}
}
