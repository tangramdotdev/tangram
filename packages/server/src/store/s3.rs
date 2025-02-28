use bytes::Bytes;
use futures::{TryStreamExt as _, stream::FuturesUnordered};
use tangram_client as tg;

pub struct S3 {
	credentials: Option<aws_credential_types::Credentials>,
	config: crate::config::S3Store,
	reqwest: reqwest::Client,
	semaphore: tokio::sync::Semaphore,
}

impl S3 {
	pub fn new(config: &crate::config::S3Store) -> Self {
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

	pub async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let _permit = self.semaphore.acquire().await;
		let method = reqwest::Method::GET;
		let url = tangram_uri::Reference::parse(self.config.url.as_str()).unwrap();
		let authority = url.authority().ok_or_else(|| tg::error!("invalid url"))?;
		let bucket = &self.config.bucket;
		let url = url
			.to_builder()
			.authority(format!("{bucket}.{authority}"))
			.path(format!("/{id}"))
			.build()
			.unwrap();
		let request = self.reqwest.request(method, url.as_str()).build().unwrap();
		let request = self.sign_request(request)?;
		let response = self
			.reqwest
			.execute(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let text = response
				.text()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the response body"))?;
			return Err(tg::error!(%text, "the request failed"));
		}
		let bytes = response
			.bytes()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the response body"))?;
		Ok(Some(bytes))
	}

	pub async fn put(&self, id: &tg::object::Id, bytes: Bytes) -> tg::Result<()> {
		let _permit = self.semaphore.acquire().await;
		let method = reqwest::Method::PUT;
		let url = tangram_uri::Reference::parse(self.config.url.as_str()).unwrap();
		let authority = url.authority().ok_or_else(|| tg::error!("invalid url"))?;
		let bucket = &self.config.bucket;
		let url = url
			.to_builder()
			.authority(format!("{bucket}.{authority}"))
			.path(format!("/{id}"))
			.build()
			.unwrap();
		let request = self
			.reqwest
			.request(method, url.as_str())
			.header(http::header::CONTENT_LENGTH, bytes.len().to_string())
			.header(http::header::IF_MATCH, "")
			.body(bytes)
			.build()
			.unwrap();
		let request = self.sign_request(request)?;
		let response = self
			.reqwest
			.execute(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::PRECONDITION_FAILED {
			return Ok(());
		}
		if !response.status().is_success() {
			let text = response
				.text()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the response body"))?;
			return Err(tg::error!(%text, "the request failed"));
		}
		Ok(())
	}

	pub async fn put_batch(&self, items: &[(tg::object::Id, Bytes)]) -> tg::Result<()> {
		items
			.iter()
			.map(|(id, bytes)| self.put(id, bytes.clone()))
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await
	}

	fn sign_request(&self, request: reqwest::Request) -> tg::Result<reqwest::Request> {
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
			.map_err(|source| tg::error!(!source, "failed to sign the request"))?;
		let (signing_instructions, _signature) = signing_output.into_parts();
		let mut request = http::Request::try_from(request)
			.map_err(|source| tg::error!(!source, "failed to convert the request"))?;
		signing_instructions.apply_to_request_http1x(&mut request);
		let request = reqwest::Request::try_from(request)
			.map_err(|source| tg::error!(!source, "failed to convert the request"))?;
		Ok(request)
	}
}
