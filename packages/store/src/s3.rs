use {
	crate::{CacheReference, DeleteArg, Error as _, PutArg},
	bytes::Bytes,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	tangram_client as tg,
	time::format_description::well_known::Rfc2822,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub access_key: Option<String>,
	pub bucket: String,
	pub region: Option<String>,
	pub secret_key: Option<String>,
	pub url: tangram_uri::Uri,
}

pub struct Store {
	credentials: Option<aws_credential_types::Credentials>,
	config: Config,
	reqwest: reqwest::Client,
	semaphore: tokio::sync::Semaphore,
}

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Reqwest(reqwest::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	#[must_use]
	pub fn new(config: &Config) -> Self {
		let credentials = if let (Some(access_key), Some(secret_key)) =
			(&config.access_key, &config.secret_key)
		{
			let credentials = aws_credential_types::Credentials::new(
				access_key.as_str(),
				secret_key.as_str(),
				None,
				None,
				"",
			);
			Some(credentials)
		} else {
			None
		};
		let reqwest = reqwest::Client::new();
		let semaphore = tokio::sync::Semaphore::new(256);
		Self {
			credentials,
			config: config.clone(),
			reqwest,
			semaphore,
		}
	}

	fn sign_request(&self, request: reqwest::Request) -> Result<reqwest::Request, Error> {
		let mut signing_params_builder = aws_sigv4::sign::v4::SigningParams::builder();
		let identity = self.credentials.clone().map(Into::into);
		if let Some(identity) = &identity {
			signing_params_builder = signing_params_builder.identity(identity);
		}
		signing_params_builder = signing_params_builder.name("");
		if let Some(region) = &self.config.region {
			signing_params_builder = signing_params_builder.region(region);
		}
		let signing_settings = aws_sigv4::http_request::SigningSettings::default();
		signing_params_builder = signing_params_builder.settings(signing_settings);
		let time = std::time::SystemTime::now();
		signing_params_builder = signing_params_builder.time(time);
		let signing_params = signing_params_builder.build().unwrap().into();
		let method = request.method().as_str();
		let uri = request.url().as_str();
		let headers = std::iter::empty();
		let body = if let Some(body) = request.body() {
			if let Some(bytes) = body.as_bytes() {
				aws_sigv4::http_request::SignableBody::Bytes(bytes)
			} else {
				aws_sigv4::http_request::SignableBody::UnsignedPayload
			}
		} else {
			aws_sigv4::http_request::SignableBody::empty()
		};
		let signable_request =
			aws_sigv4::http_request::SignableRequest::new(method, uri, headers, body).unwrap();
		let signing_output = aws_sigv4::http_request::sign(signable_request, &signing_params)
			.map_err(|error| Error::other(format!("failed to sign the request: {error}")))?;
		let (signing_instructions, _signature) = signing_output.into_parts();
		let mut request = http::Request::try_from(request)
			.map_err(|error| Error::other(format!("failed to convert the request: {error}")))?;
		signing_instructions.apply_to_request_http1x(&mut request);
		let request = reqwest::Request::try_from(request)
			.map_err(|error| Error::other(format!("failed to convert the request: {error}")))?;
		Ok(request)
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		let _permit = self.semaphore.acquire().await;
		let method = reqwest::Method::GET;
		let url = tangram_uri::Uri::parse(self.config.url.as_str()).unwrap();
		let authority = url.authority().ok_or_else(|| Error::other("invalid url"))?;
		let bucket = &self.config.bucket;
		let url = url
			.to_builder()
			.authority(format!("{bucket}.{authority}"))
			.path(format!("/{id}"))
			.build()
			.unwrap();
		let request = self
			.reqwest
			.request(method, url.to_string())
			.build()
			.unwrap();
		let request = self.sign_request(request).map_err(Error::other)?;
		let response = self.reqwest.execute(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let text = response.text().await?;
			return Err(Error::other(format!("the request failed: {text}")));
		}
		let bytes = response.bytes().await?;
		Ok(Some(bytes))
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		ids.iter()
			.map(|id| self.try_get(id))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}

	async fn try_get_cache_reference(
		&self,
		_: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		Ok(None)
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		let _permit = self.semaphore.acquire().await;
		let method = reqwest::Method::PUT;
		let url = tangram_uri::Uri::parse(self.config.url.as_str()).unwrap();
		let authority = url.authority().ok_or_else(|| Error::other("invalid url"))?;
		let bucket = &self.config.bucket;
		let url = url
			.to_builder()
			.authority(format!("{bucket}.{authority}"))
			.path(format!("/{id}", id = arg.id))
			.build()
			.unwrap();
		let request = self
			.reqwest
			.request(method, url.to_string())
			.header(
				http::header::CONTENT_LENGTH,
				arg.bytes.as_ref().unwrap().len().to_string(),
			)
			.body(arg.bytes.unwrap())
			.build()
			.unwrap();
		let request = self.sign_request(request).map_err(Error::other)?;
		let response = self.reqwest.execute(request).await?;
		if !response.status().is_success() {
			let text = response.text().await?;
			return Err(Error::other(format!("the request failed: {text}")));
		}
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		args.into_iter()
			.map(|arg| self.put(arg))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		let _permit = self.semaphore.acquire().await;
		let method = reqwest::Method::DELETE;
		let url = tangram_uri::Uri::parse(self.config.url.as_str()).unwrap();
		let authority = url.authority().ok_or_else(|| Error::other("invalid url"))?;
		let bucket = &self.config.bucket;
		let url = url
			.to_builder()
			.authority(format!("{bucket}.{authority}"))
			.path(format!("/{id}", id = arg.id))
			.build()
			.unwrap();
		let if_unmodified_since =
			time::OffsetDateTime::from_unix_timestamp(arg.now - arg.ttl.to_i64().unwrap())
				.unwrap()
				.format(&Rfc2822)
				.unwrap();
		let request = self
			.reqwest
			.request(method, url.to_string())
			.header(http::header::IF_UNMODIFIED_SINCE, if_unmodified_since)
			.build()
			.unwrap();
		let request = self.sign_request(request).map_err(Error::other)?;
		let response = self.reqwest.execute(request).await?;
		if !response.status().is_success() {
			let text = response.text().await?;
			return Err(Error::other(format!("the request failed: {text}")));
		}
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		args.into_iter()
			.map(|arg| self.delete(arg))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		Ok(())
	}
}

impl crate::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}
