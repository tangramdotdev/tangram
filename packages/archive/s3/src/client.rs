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
	request_timeout: Duration,
	session: Option<tokio::sync::Mutex<Option<Session>>>,
	session_refresh_buffer: time::Duration,
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
		if config.express && config.path_style {
			return Err(tg::error!(
				"the S3 Express archive does not support path-style addressing"
			));
		}
		if config.connect_timeout.is_zero() {
			return Err(tg::error!(
				"the S3 archive connect timeout must be greater than zero"
			));
		}
		if config.request_timeout.is_zero() {
			return Err(tg::error!(
				"the S3 archive request timeout must be greater than zero"
			));
		}
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
		let (authority, path) = if config.path_style {
			(authority.to_owned(), format!("/{}", config.bucket))
		} else {
			(format!("{}.{authority}", config.bucket), String::new())
		};
		let url = config
			.endpoint
			.to_builder()
			.authority(&authority)
			.path(&path)
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the S3 archive URL"))?;
		let credentials = Credentials::new(
			config.access_key.clone(),
			config.secret_key.clone(),
			None,
			None,
			"tangram",
		);
		let pool = Self::pool(config.pool, &config.reconnect, &url, config.connect_timeout);
		let region = config.region.clone();
		let session_refresh_buffer = time::Duration::try_from(config.session_refresh_buffer)
			.map_err(|error| {
				tg::error!(
					!error,
					"the S3 archive session refresh buffer is out of range"
				)
			})?;

		Ok(Self {
			credentials,
			express: config.express,
			pool,
			region,
			request_timeout: config.request_timeout,
			session,
			session_refresh_buffer,
			url,
		})
	}

	fn pool(
		options: tangram_pool::Options,
		reconnect: &tangram_futures::retry::Options,
		url: &Uri,
		connect_timeout: Duration,
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
						match Self::connect(&url, connect_timeout).await {
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
