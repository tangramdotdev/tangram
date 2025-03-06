use super::Server;
use futures::future;
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
	blobs: Vec<tg::blob::Id>,
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

		// Clean until there are no more processes or objects to remove.
		let config = self.config.cleaner.clone().unwrap_or_default();
		loop {
			let output = self.cleaner_task_inner(&config).await?;
			let n = output.processes.len() + output.objects.len() + output.blobs.len();
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
					let n = output.processes.len() + output.objects.len() + output.blobs.len();
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

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes.
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
		let max_touched_at =
			time::OffsetDateTime::from_unix_timestamp(now - config.ttl.as_secs().to_i64().unwrap())
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
		let params = db::params![max_touched_at, config.batch_size];
		let processes = connection
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Delete objects.
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
		let max_touched_at =
			time::OffsetDateTime::from_unix_timestamp(now - config.ttl.as_secs().to_i64().unwrap())
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
		let params = db::params![max_touched_at, config.batch_size];
		let objects = connection
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Delete blobs.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from blobs
				where id in (
					select id from blobs
					where reference_count = 0
					limit {p}1
				)
				returning id;
			"
		);
		let params = db::params![config.batch_size];
		let blobs = connection
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		let arg = crate::store::DeleteBatchArg {
			ids: objects.clone(),
			now,
			ttl: config.ttl.as_secs(),
		};
		self.store.delete_batch(arg).await?;
		future::try_join_all(blobs.iter().map(|blob: &tg::blob::Id| async {
			let path = self.blobs_path().join(blob.to_string());
			tokio::fs::remove_file(&path)
				.await
				.map_err(|source| tg::error!(!source, ?path, "failed to remove the blob"))?;
			Ok::<_, tg::Error>(())
		}))
		.await?;

		let output = InnerOutput {
			processes,
			objects,
			blobs,
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
