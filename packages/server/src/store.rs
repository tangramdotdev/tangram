use aws_sdk_s3 as s3;
use bytes::Bytes;
use dashmap::DashMap;
use tangram_client as tg;

pub enum Store {
	Memory(DashMap<tg::object::Id, Bytes, fnv::FnvBuildHasher>),
	S3(S3),
}

pub struct S3 {
	credentials: Option<aws_credential_types::Credentials>,
	aws: s3::Client,
	config: crate::config::S3Store,
	reqwest: reqwest::Client,
	semaphore: tokio::sync::Semaphore,
}

impl Store {
	pub fn new_memory() -> Self {
		Self::Memory(DashMap::default())
	}

	pub fn new_s3(config: &crate::config::S3Store) -> Self {
		Self::S3(S3::new(config))
	}

	pub async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		match self {
			Self::Memory(map) => Ok(map.get(id).map(|value| value.clone())),
			Self::S3(s3) => s3.try_get(id).await,
		}
	}

	pub async fn put(&self, id: tg::object::Id, bytes: Bytes) -> tg::Result<()> {
		match self {
			Self::Memory(map) => {
				map.insert(id, bytes);
			},
			Self::S3(s3) => {
				s3.put(id, bytes).await?;
			},
		}
		Ok(())
	}
}

impl S3 {
	pub fn new(config: &crate::config::S3Store) -> Self {
		let mut builder = s3::Config::builder();
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
		builder.set_credentials_provider(
			credentials
				.clone()
				.map(aws_credential_types::provider::SharedCredentialsProvider::new),
		);
		builder.set_behavior_version(Some(s3::config::BehaviorVersion::latest()));
		builder.set_stalled_stream_protection(Some(
			s3::config::StalledStreamProtectionConfig::disabled(),
		));
		builder.set_force_path_style(true.into());
		builder.set_endpoint_url(Some(config.url.to_string()));
		if let Some(region) = config.region.clone() {
			builder.set_region(Some(s3::config::Region::new(region)));
		}
		let aws = s3::Client::from_conf(builder.build());
		let reqwest = reqwest::Client::new();
		let semaphore = tokio::sync::Semaphore::new(256);
		Self {
			credentials,
			aws,
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

	pub async fn put(&self, id: tg::object::Id, bytes: Bytes) -> tg::Result<()> {
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
			.header("If-Match", "")
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

	pub async fn try_get_aws(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let _permit = self.semaphore.acquire().await;
		let response = self
			.aws
			.get_object()
			.bucket(&self.config.bucket)
			.key(id.to_string())
			.send()
			.await;
		let output = match response {
			Ok(output) => output,
			Err(s3::error::SdkError::ServiceError(error)) => match error.into_err() {
				s3::operation::get_object::GetObjectError::NoSuchKey(_) => return Ok(None),
				error => {
					return Err(tg::error!(!error, "failed to get the object"));
				},
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to get the object"));
			},
		};
		let bytes = output
			.body
			.collect()
			.await
			.map(s3::primitives::AggregatedBytes::into_bytes)
			.map_err(|source| tg::error!(!source, "failed to read the object"))?;
		Ok(Some(bytes))
	}

	pub async fn put_aws(&self, id: tg::object::Id, bytes: Bytes) -> tg::Result<()> {
		let _permit = self.semaphore.acquire().await;
		let bytes = s3::primitives::SdkBody::from(bytes.to_vec());
		let result = self
			.aws
			.put_object()
			.bucket(&self.config.bucket)
			.key(id.to_string())
			.if_match("")
			.body(bytes.into())
			.send()
			.await;
		match result {
			Ok(_) => (),
			Err(error) => {
				if let Some(response) = error.raw_response() {
					if response.status().as_u16() == 412 {
						return Ok(());
					}
				}
				return Err(tg::error!(!error, "failed to put the object"));
			},
		}
		Ok(())
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
