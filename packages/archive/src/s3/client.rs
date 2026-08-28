use {
	self::connection::Connection,
	aws_credential_types::Credentials,
	bytes::Bytes,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

mod connection;
mod request;
mod session;
mod sign;

type Pool = tangram_pool::Pool<Connection, tg::Error>;

pub(super) struct Client {
	credentials: Credentials,
	express: bool,
	pool: Pool,
	region: String,
	session: Option<tokio::sync::Mutex<Option<Session>>>,
	url: Uri,
}

pub(super) struct Response {
	pub bytes: Bytes,
	pub headers: http::HeaderMap,
	pub status: http::StatusCode,
}

pub(super) struct Session {
	credentials: Credentials,
	expiration: time::OffsetDateTime,
	token: String,
}

impl Client {
	#[must_use = "the S3 client construction result must be checked"]
	pub(super) fn new(config: &super::Config) -> tg::Result<Self> {
		if config.pool.max == 0 {
			return Err(tg::error!(
				"the S3 archive pool maximum must be greater than zero"
			));
		}
		if config.pool.min > config.pool.max {
			return Err(tg::error!(
				"the S3 archive pool minimum exceeds its maximum"
			));
		}
		if !matches!(config.endpoint.scheme(), Some("http" | "https")) {
			return Err(tg::error!("the S3 archive endpoint must use HTTP or HTTPS"));
		}
		let authority = config
			.endpoint
			.authority()
			.ok_or_else(|| tg::error!("the S3 archive endpoint has no authority"))?;
		let session = config.express.then(Default::default);
		if !matches!(config.endpoint.path(), "" | "/")
			|| config.endpoint.query().is_some()
			|| config.endpoint.fragment().is_some()
		{
			return Err(tg::error!(
				"the S3 archive endpoint must not have a path, query, or fragment"
			));
		}
		let authority = format!("{}.{authority}", config.bucket);
		let url = config
			.endpoint
			.to_builder()
			.authority(&authority)
			.path("")
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the S3 archive URL"))?;
		let credentials = Credentials::new(
			config.access_key.clone(),
			config.secret_key.clone(),
			None,
			None,
			"tangram",
		);
		let pool = Self::pool(config.pool, &config.reconnect, &url);
		let region = config.region.clone();

		Ok(Self {
			credentials,
			express: config.express,
			pool,
			region,
			session,
			url,
		})
	}

	fn pool(
		options: tangram_pool::Options,
		reconnect: &tangram_futures::retry::Options,
		url: &Uri,
	) -> Pool {
		let reconnect = reconnect.clone();
		let url = url.clone();
		Pool::new(options, move || {
			let reconnect = reconnect.clone();
			let url = url.clone();
			async move {
				tangram_futures::retry(&reconnect, || {
					let url = url.clone();
					async move {
						match Self::connect(&url).await {
							Ok(connection) => Ok(ControlFlow::Break(connection)),
							Err(error) => Ok(ControlFlow::Continue(error)),
						}
					}
				})
				.await
			}
		})
	}
}

const REQUEST_TIMEOUT: Duration = Duration::from_mins(1);
