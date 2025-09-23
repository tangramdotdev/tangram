use {
	crate::{
		Body, Request, Response,
		body::compression::{Algorithm, Compression, Decompression},
		header::{accept_encoding::AcceptEncoding, content_encoding::ContentEncoding},
	},
	std::sync::Arc,
	tower::ServiceExt as _,
};

#[derive(Clone, Default)]
pub struct RequestCompressionLayer {
	predicate: Option<
		Arc<
			dyn Fn(&http::request::Parts, &Body) -> Option<(Algorithm, u32)>
				+ Send
				+ Sync
				+ 'static,
		>,
	>,
}

impl RequestCompressionLayer {
	pub fn new<F>(predicate: F) -> Self
	where
		F: Fn(&http::request::Parts, &Body) -> Option<(Algorithm, u32)>
			+ Send
			+ Sync
			+ 'static + 'static,
	{
		Self {
			predicate: Some(Arc::new(predicate)),
		}
	}
}

impl<S> tower::layer::Layer<S> for RequestCompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		let layer = self.clone();
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let layer = layer.clone();
			let mut service = service.clone();
			async move {
				let (mut parts, mut body) = request.into_parts();
				let has_content_encoding =
					parts.headers.contains_key(http::header::CONTENT_ENCODING);
				if !has_content_encoding {
					if let Some(predicate) = &layer.predicate {
						if let Some((algorithm, level)) = (predicate)(&parts, &body) {
							parts.headers.remove(http::header::CONTENT_LENGTH);
							parts.headers.insert(
								http::header::CONTENT_ENCODING,
								http::HeaderValue::from_str(&algorithm.to_string()).unwrap(),
							);
							body = Body::new(Compression::new(body, algorithm, level));
						}
					}
				}
				let request = Request::from_parts(parts, body);
				let response = service.ready().await?.call(request).await?;
				Ok(response)
			}
		}))
	}
}

#[derive(Default)]
pub struct RequestDecompressionLayer;

impl<S> tower::layer::Layer<S> for RequestDecompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let mut service = service.clone();
			async move {
				let (mut parts, mut body) = request.into_parts();
				let encoding = parts
					.headers
					.get(http::header::CONTENT_ENCODING)
					.and_then(|value| value.to_str().ok())
					.and_then(|value| value.parse::<ContentEncoding>().ok());
				if encoding.is_some() {
					parts.headers.remove(http::header::CONTENT_ENCODING);
					parts.headers.remove(http::header::CONTENT_LENGTH);
				}
				let algorithm = match encoding {
					Some(ContentEncoding::Gzip) => Some(Algorithm::Gzip),
					Some(ContentEncoding::Zstd) => Some(Algorithm::Zstd),
					None => None,
				};
				if let Some(algorithm) = algorithm {
					body = Body::new(Decompression::new(body, algorithm));
				}
				let request = Request::from_parts(parts, body);
				let response = service.ready().await?.call(request).await?;
				Ok(response)
			}
		}))
	}
}

#[derive(Clone, Default)]
pub struct ResponseCompressionLayer {
	pub predicate: Option<
		Arc<
			dyn Fn(
					Option<AcceptEncoding>,
					&http::response::Parts,
					&Body,
				) -> Option<(Algorithm, u32)>
				+ Send
				+ Sync
				+ 'static,
		>,
	>,
}

impl ResponseCompressionLayer {
	pub fn new<F>(predicate: F) -> Self
	where
		F: Fn(Option<AcceptEncoding>, &http::response::Parts, &Body) -> Option<(Algorithm, u32)>
			+ Send
			+ Sync
			+ 'static,
	{
		Self {
			predicate: Some(Arc::new(predicate)),
		}
	}
}

impl<S> tower::layer::Layer<S> for ResponseCompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		let layer = self.clone();
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: Request| {
			let layer = layer.clone();
			let mut service = service.clone();
			async move {
				let accept_encoding: Option<AcceptEncoding> = request
					.headers()
					.get(http::header::ACCEPT_ENCODING)
					.and_then(|value| value.to_str().ok())
					.and_then(|value| value.parse().ok());
				let response = service.ready().await?.call(request).await?;
				let (mut parts, mut body) = response.into_parts();
				let has_content_encoding =
					parts.headers.contains_key(http::header::CONTENT_ENCODING);
				if !has_content_encoding {
					if let Some(predicate) = &layer.predicate {
						if let Some((algorithm, level)) =
							(predicate)(accept_encoding, &parts, &body)
						{
							parts.headers.remove(http::header::CONTENT_LENGTH);
							parts.headers.insert(
								http::header::CONTENT_ENCODING,
								http::HeaderValue::from_str(&algorithm.to_string()).unwrap(),
							);
							body = Body::new(Compression::new(body, algorithm, level));
						}
					}
				}
				let response = Response::from_parts(parts, body);
				Ok(response)
			}
		}))
	}
}

#[derive(Default)]
pub struct ResponseDecompressionLayer;

impl<S> tower::layer::Layer<S> for ResponseDecompressionLayer
where
	S: tower::Service<Request, Response = Response> + Clone + Send + Sync + 'static,
	S::Future: Send + 'static,
	S::Error: Send + 'static,
{
	type Service = tower::util::BoxCloneSyncService<Request, Response, S::Error>;

	fn layer(&self, service: S) -> Self::Service {
		tower::util::BoxCloneSyncService::new(tower::service_fn(move |mut request: Request| {
			let mut service = service.clone();
			async move {
				request.headers_mut().insert(
					http::header::ACCEPT_ENCODING,
					http::HeaderValue::from_str(&ContentEncoding::Zstd.to_string()).unwrap(),
				);
				let response = service.ready().await?.call(request).await?;
				let (mut parts, mut body) = response.into_parts();
				let encoding = parts
					.headers
					.get(http::header::CONTENT_ENCODING)
					.and_then(|value| value.to_str().ok())
					.and_then(|value| value.parse::<ContentEncoding>().ok());
				if encoding.is_some() {
					parts.headers.remove(http::header::CONTENT_ENCODING);
					parts.headers.remove(http::header::CONTENT_LENGTH);
				}
				let algorithm = match encoding {
					Some(ContentEncoding::Gzip) => Some(Algorithm::Gzip),
					Some(ContentEncoding::Zstd) => Some(Algorithm::Zstd),
					None => None,
				};
				if let Some(algorithm) = algorithm {
					body = Body::new(Decompression::new(body, algorithm));
				}
				let response = Response::from_parts(parts, body);
				Ok(response)
			}
		}))
	}
}
