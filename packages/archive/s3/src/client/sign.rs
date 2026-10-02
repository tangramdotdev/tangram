use {
	super::Client, aws_credential_types::Credentials, aws_sigv4::http_request, bytes::Bytes,
	tangram_client::prelude::*,
};

pub(super) struct Request<'a> {
	pub body: Bytes,
	pub credentials: &'a Credentials,
	pub headers: http::HeaderMap,
	pub method: http::Method,
	pub path: &'a str,
	pub query: Option<&'a str>,
	pub session_token: Option<&'a str>,
}

impl Client {
	pub(super) fn request(
		&self,
		arg: Request<'_>,
	) -> tg::Result<http::Request<tangram_http::body::Boxed>> {
		let Request {
			body,
			credentials,
			headers,
			method,
			path,
			query,
			session_token,
		} = arg;
		let path = format!("{}{path}", self.url.path());
		let mut builder = self.url.to_builder().path(&path);
		if let Some(query) = query {
			builder = builder.query_raw(query);
		}
		let url = builder
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the S3 request URL"))?;
		let authority = url
			.authority()
			.ok_or_else(|| tg::error!("the S3 request URL has no authority"))?;
		let mut request = http::Request::builder()
			.method(method)
			.uri(url.as_str())
			.header(http::header::HOST, authority)
			.body(body)
			.map_err(|error| tg::error!(!error, "failed to build the S3 request"))?;
		request.headers_mut().extend(headers);
		if let Some(session_token) = session_token {
			let session_token = http::HeaderValue::from_str(session_token).map_err(|error| {
				tg::error!(
					!error,
					"failed to create the S3 Express session-token header"
				)
			})?;
			request.headers_mut().insert(
				http::HeaderName::from_static("x-amz-s3session-token"),
				session_token,
			);
		}

		let identity = credentials.clone().into();
		let signing_name = signing_name(self.express);
		let mut settings = http_request::SigningSettings::default();
		settings
			.excluded_headers
			.get_or_insert_default()
			.push(std::borrow::Cow::Borrowed(
				http::header::CONTENT_LENGTH.as_str(),
			));
		settings.payload_checksum_kind = http_request::PayloadChecksumKind::XAmzSha256;
		settings.percent_encoding_mode = http_request::PercentEncodingMode::Single;
		settings.uri_path_normalization_mode = http_request::UriPathNormalizationMode::Disabled;
		let params = aws_sigv4::sign::v4::SigningParams::builder()
			.identity(&identity)
			.name(signing_name)
			.region(&self.region)
			.settings(settings)
			.time(std::time::SystemTime::now())
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the S3 signing parameters"))?
			.into();
		let headers = request
			.headers()
			.iter()
			.map(|(name, value)| {
				let value = value.to_str().map_err(
					|error| tg::error!(!error, header = %name, "the S3 request header is invalid"),
				)?;

				Ok((name.as_str(), value))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let body = http_request::SignableBody::Bytes(request.body());
		let signable = http_request::SignableRequest::new(
			request.method().as_str(),
			request.uri().to_string(),
			headers.into_iter(),
			body,
		)
		.map_err(|error| tg::error!(!error, "failed to create the signable S3 request"))?;
		let output = http_request::sign(signable, &params)
			.map_err(|error| tg::error!(!error, "failed to sign the S3 request"))?;
		let (instructions, _) = output.into_parts();
		instructions.apply_to_request_http1x(&mut request);

		// Send an origin-form HTTP/1 target after signing the absolute S3 URL.
		let target = match query {
			Some(query) => format!("{path}?{query}"),
			None => path,
		};
		let uri = target
			.parse()
			.map_err(|error| tg::error!(!error, "failed to create the S3 request target"))?;
		*request.uri_mut() = uri;
		let (parts, body) = request.into_parts();
		let body = tangram_http::body::Boxed::with_bytes(body);
		let request = http::Request::from_parts(parts, body);

		Ok(request)
	}
}

fn signing_name(express: bool) -> &'static str {
	if express { "s3express" } else { "s3" }
}

#[cfg(test)]
mod tests {
	use {aws_credential_types::Credentials, bytes::Bytes};

	fn config() -> super::super::super::Config {
		super::super::super::Config {
			access_key: "root-access".into(),
			bucket: "bucket--use1-az4--x-s3".into(),
			endpoint: tangram_uri::Uri::parse("https://objects.example.com").unwrap(),
			express: true,
			path_style: false,
			pool: tangram_pool::Options::default(),
			reconnect: tangram_futures::retry::Options::default(),
			region: "us-east-1".into(),
			secret_key: "root-secret".into(),
		}
	}

	fn client() -> super::super::Client {
		let config = config();
		super::super::Client::new(&config).unwrap()
	}

	#[test]
	fn signs_requests_with_the_selected_bucket_addressing() {
		for path_style in [false, true] {
			let mut config = config();
			config.bucket = "archive-bucket".into();
			config.endpoint = tangram_uri::Uri::parse("https://objects.example.com:9443").unwrap();
			config.express = false;
			config.path_style = path_style;
			let client = super::super::Client::new(&config).unwrap();
			let credentials = Credentials::new("access", "secret", None, None, "test");
			for method in [http::Method::GET, http::Method::PUT, http::Method::DELETE] {
				let arg = super::Request {
					body: Bytes::from_static(b"object contents"),
					credentials: &credentials,
					headers: http::HeaderMap::new(),
					method: method.clone(),
					path: "/key",
					query: Some("versionId=version"),
					session_token: None,
				};
				let request = client.request(arg).unwrap();
				let (host, target) = if path_style {
					(
						"objects.example.com:9443",
						"/archive-bucket/key?versionId=version",
					)
				} else {
					(
						"archive-bucket.objects.example.com:9443",
						"/key?versionId=version",
					)
				};
				assert_eq!(request.method(), method);
				assert_eq!(request.headers()[http::header::HOST], host);
				assert_eq!(request.uri(), target);
				let authorization = request.headers()[http::header::AUTHORIZATION]
					.to_str()
					.unwrap();
				assert!(authorization.contains("/s3/aws4_request"));
				assert!(!request.headers().contains_key("x-amz-s3session-token"));
			}
		}
	}

	#[test]
	fn rejects_path_style_for_express() {
		let mut config = config();
		config.path_style = true;
		let error = super::super::Client::new(&config).err().unwrap();
		assert_eq!(
			error.to_string(),
			"the S3 Express archive does not support path-style addressing"
		);
	}

	#[test]
	fn signs_an_express_request_with_session_credentials() {
		let client = client();
		let credentials = Credentials::new("session-access", "session-secret", None, None, "test");
		let arg = super::Request {
			body: Bytes::new(),
			credentials: &credentials,
			headers: http::HeaderMap::new(),
			method: http::Method::GET,
			path: "/key",
			query: None,
			session_token: Some("session-token"),
		};
		let request = client.request(arg).unwrap();
		let authorization = request
			.headers()
			.get(http::header::AUTHORIZATION)
			.unwrap()
			.to_str()
			.unwrap();
		assert!(authorization.contains("Credential=session-access/"));
		assert!(authorization.contains("/s3express/aws4_request"));
		assert!(authorization.contains("x-amz-s3session-token"));
		assert_eq!(request.headers()["x-amz-s3session-token"], "session-token");
		assert_eq!(request.uri(), "/key");
	}

	#[test]
	fn signs_a_create_session_request_with_root_credentials() {
		let client = client();
		let credentials = Credentials::new("root-access", "root-secret", None, None, "test");
		let arg = super::Request {
			body: Bytes::new(),
			credentials: &credentials,
			headers: http::HeaderMap::new(),
			method: http::Method::GET,
			path: "/",
			query: Some("session"),
			session_token: None,
		};
		let request = client.request(arg).unwrap();
		let authorization = request
			.headers()
			.get(http::header::AUTHORIZATION)
			.unwrap()
			.to_str()
			.unwrap();
		assert!(authorization.contains("Credential=root-access/"));
		assert!(authorization.contains("/s3express/aws4_request"));
		assert!(!request.headers().contains_key("x-amz-s3session-token"));
		assert_eq!(request.uri(), "/?session");
	}

	#[test]
	fn signing_name() {
		assert_eq!(super::signing_name(false), "s3");
		assert_eq!(super::signing_name(true), "s3express");
	}
}
