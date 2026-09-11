use {
	crate::Server,
	futures::{TryStreamExt as _, stream},
	std::ops::ControlFlow,
	tangram_archive::{self as archive, Archive as _},
	tangram_cache::Cache as _,
	tangram_client::prelude::*,
};

pub use archive::object;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Archive {
	S3(archive::s3::Archive),
}

impl Archive {
	#[must_use = "the archive construction result must be checked"]
	pub fn new_s3(config: &crate::config::S3Archive) -> tg::Result<Self> {
		let pool = tangram_pool::Options {
			max: config.pool.max,
			min: config.pool.min,
			shared: 1,
			ttl: config.pool.ttl,
		};
		let reconnect = tangram_futures::retry::Options {
			backoff: config.reconnect.backoff,
			jitter: config.reconnect.jitter,
			max_delay: config.reconnect.max_delay,
			max_retries: config.reconnect.max_retries,
		};
		let config = archive::s3::Config {
			access_key: config.access_key.clone(),
			bucket: config.bucket.clone(),
			endpoint: config.endpoint.clone(),
			express: config.express,
			pool,
			reconnect,
			region: config.region.clone(),
			secret_key: config.secret_key.clone(),
		};
		let archive = archive::s3::Archive::new(&config)?;

		Ok(Self::S3(archive))
	}
}

impl Server {
	pub(crate) fn archive_object_batch_task(&self, args: Vec<object::put::Arg>) {
		if args.is_empty() {
			return;
		}
		self.archive_tasks
			.spawn({
				let server = self.clone();
				|_| async move {
					stream::iter(args.into_iter().map(Ok))
						.try_for_each_concurrent(
							server.config.object.archive_queue.concurrency,
							|arg| server.archive_object_with_retry(arg),
						)
						.await
				}
			})
			.detach();
	}

	async fn archive_object_with_retry(&self, arg: object::put::Arg) -> tg::Result<()> {
		tangram_futures::retry(&crate::indexer::RETRY_OPTIONS, || {
			let arg = arg.clone();
			async move {
				match self.archive_object(arg).await {
					Ok(()) => Ok(ControlFlow::Break(())),
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to archive an object");
						Ok(ControlFlow::Continue(error))
					},
				}
			}
		})
		.await?;
		Ok(())
	}

	pub(crate) async fn archive_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		let archive = self
			.archive
			.as_ref()
			.ok_or_else(|| tg::error!("the archive is unavailable"))?;
		let id = arg.id.clone();
		let put = arg.put;
		archive
			.put_object(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put an object in the archive"))?;
		if let Some(config) = &self.config.object.cache {
			let arg = crate::cache::object::cache::put::Arg {
				cache: uuid::Uuid::now_v7().into_bytes(),
				id,
				partition: rand::random_range(0..config.partition_total),
				put,
			};
			self.cache.put_object_cache_entry(arg).await?;
		}

		Ok(())
	}
}

impl archive::Archive for Archive {
	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		match self {
			Self::S3(archive) => archive.delete_object(arg).await,
		}
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		match self {
			Self::S3(archive) => archive.delete_object_batch(args).await,
		}
	}

	async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			Self::S3(archive) => archive.put_object(arg).await,
		}
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		match self {
			Self::S3(archive) => archive.try_get_object(arg).await,
		}
	}
}
