use {tangram_archive as archive, tangram_client::prelude::*};

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
