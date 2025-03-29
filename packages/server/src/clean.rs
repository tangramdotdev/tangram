use super::Server;
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};
use time::format_description::well_known::Rfc3339;

struct InnerOutput {
	processes: Vec<tg::process::Id>,
	objects: Vec<tg::object::Id>,
	cache_entries: Vec<tg::artifact::Id>,
}

impl Server {
	pub async fn clean(&self) -> tg::Result<()> {
		// Clean the temporary directory.
		tokio::fs::remove_dir_all(self.temp_path())
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the temporary directory"))?;
		tokio::fs::create_dir_all(self.temp_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the temporary directory")
			})?;

		// Clean until there are no more items to remove.
		let batch_size = self
			.config
			.cleaner
			.as_ref()
			.map_or(1024, |config| config.batch_size);
		let ttl = Duration::from_secs(0);
		let config = crate::config::Cleaner { batch_size, ttl };
		loop {
			let output = self.cleaner_task_inner(&config).await?;
			let n = output.processes.len() + output.objects.len() + output.cache_entries.len();
			if n == 0 {
				break;
			}
		}

		Ok(())
	}

	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		loop {
			let result = self.cleaner_task_inner(config).await;
			match result {
				Ok(output) => {
					let n =
						output.processes.len() + output.objects.len() + output.cache_entries.len();
					if n == 0 {
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				},
				Err(error) => {
					tracing::error!(?error, "failed to clean");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn cleaner_task_inner(&self, config: &crate::config::Cleaner) -> tg::Result<InnerOutput> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let max_touched_at =
			time::OffsetDateTime::from_unix_timestamp(now - config.ttl.as_secs().to_i64().unwrap())
				.unwrap()
				.format(&Rfc3339)
				.unwrap();

		// Get an index connection.
		let connection = self
			.index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes from the index.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from processes
				where id in (
					select id from processes
					where reference_count = 0 and touched_at <= {p}1
					limit {p}2
				)
				returning id;
			"
		);
		let params = db::params![max_touched_at, config.batch_size];
		let processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Delete objects from the index.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from objects
				where id in (
					select id from objects
					where reference_count = 0 and touched_at <= {p}1
					limit {p}2
				)
				returning id;
			"
		);
		let params = db::params![max_touched_at, config.batch_size];
		let objects = connection
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Delete cache entries.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from cache_entries
				where id in (
					select id from cache_entries
					where reference_count = 0 and touched_at <= {p}1
					limit {p}2
				)
				returning id;
			"
		);
		let params = db::params![max_touched_at, config.batch_size];
		let cache_entries = connection
			.query_all_value_into::<tg::artifact::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Drop the connection.
		drop(connection);

		// Delete objects.
		let arg = crate::store::DeleteBatchArg {
			ids: objects.clone(),
			now,
			ttl: config.ttl.as_secs(),
		};
		self.store.delete_batch(arg).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes from the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from processes
				where id = {p}1 and touched_at <= {p}2;
			"
		);
		for id in &processes {
			let params = db::params![&id, max_touched_at];
			connection
				.execute(statement.clone().into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the processes"))?;
		}

		// Drop the connection.
		drop(connection);

		// Delete process logs.
		for id in &processes {
			let path = self.logs_path().join(id.to_string());
			tokio::fs::remove_file(path).await.ok();
		}

		// Delete cache entries.
		tokio::task::spawn_blocking({
			let server = self.clone();
			let cache_entries = cache_entries.clone();
			move || {
				for artifact in &cache_entries {
					let path = server.cache_path().join(artifact.to_string());
					std::fs::remove_file(&path).map_err(|source| {
						tg::error!(!source, ?path, "failed to remove the cache entry")
					})?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		let output = InnerOutput {
			processes,
			objects,
			cache_entries,
		};

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		_request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		handle.clean().await?;
		Ok(http::Response::builder().empty().unwrap())
	}
}
